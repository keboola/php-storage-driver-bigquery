<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DataExchange;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing\BigQueryDatasetSource;
use Google\Cloud\Iam\V1\Binding;
use Google\Protobuf\Any;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\LinkedBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Throwable;

class ExternalBucketLinkTest extends BaseCase
{
    private GenericBackendCredentials $kbc1Credentials;

    private GenericBackendCredentials $exBqCredentials;

    protected function setUp(): void
    {
        parent::setUp();
        // KBC1 = projects[0]: the project that registered the external bucket and wants to link it
        $this->kbc1Credentials = $this->projects[0][0];
        // exBQ = projects[1]: the external project that owns the Analytics Hub listing
        $this->exBqCredentials = $this->projects[1][0];
    }

    /**
     * Tests that LinkBucketHandler correctly handles an external Analytics Hub listing:
     * - Detects the listing is external (signaled by targetServiceAccountEmail in meta)
     * - Grants roles/analyticshub.subscriber to KBC2's SA on the listing
     * - Subscribes to the listing, creating the linked dataset in the target project
     *
     * Required GCP permissions granted in this test:
     * - KBC1's SA gets roles/analyticshub.subscriber on the exchange (to call subscribeListing)
     * - KBC1's SA gets roles/analyticshub.listingAdmin on the listing (to get/set IAM policy)
     */
    public function testLinkExternalBucket(): void
    {
        $exBqBucketResponse = $this->createTestBucket($this->exBqCredentials);
        $exBqDatasetName = $exBqBucketResponse->getCreateBucketObjectName();

        $kbc1Credentials = CredentialsHelper::getCredentialsArray($this->kbc1Credentials);
        $kbc1ProjectId = $kbc1Credentials['project_id'];

        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $linkedBucketId = $this->getTestHash() . 'in.c-Linked';
        $linkedDatasetName = $nameGenerator->createObjectNameForBucketInProject($linkedBucketId, null);

        $kbc1BqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->kbc1Credentials);

        // cleanup at beginning - delete linked dataset if it exists from previous failed run
        try {
            if ($kbc1BqClient->dataset($linkedDatasetName)->exists()) {
                $kbc1BqClient->dataset($linkedDatasetName)->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        // exBQ creates exchange + listing pointing to exBQ's dataset
        $exBqAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->exBqCredentials);
        [$dataExchange, $createdListing] = $this->prepareExternalBucketForRegistration(
            $exBqAnalyticHubClient,
            $this->exBqCredentials,
            $exBqDatasetName,
        );

        // exBQ grants roles/analyticshub.subscriber on the exchange to KBC1's SA
        // so KBC1 can call subscribeListing
        $iamExchangePolicy = $exBqAnalyticHubClient->getIamPolicy($dataExchange->getName());
        $exchangeBindings = $iamExchangePolicy->getBindings();
        $exchangeBindings[] = new Binding([
            'role' => 'roles/analyticshub.subscriber',
            'members' => ['serviceAccount:' . $kbc1Credentials['client_email']],
        ]);
        $iamExchangePolicy->setBindings($exchangeBindings);
        $exBqAnalyticHubClient->setIamPolicy($dataExchange->getName(), $iamExchangePolicy);

        // exBQ grants roles/analyticshub.listingAdmin on the listing to KBC1's SA
        // so KBC1 can grant subscriber access to KBC2 (get/set IAM policy on listing)
        $iamListingPolicy = $exBqAnalyticHubClient->getIamPolicy($createdListing->getName());
        $listingBindings = $iamListingPolicy->getBindings();
        $listingBindings[] = new Binding([
            'role' => 'roles/analyticshub.listingAdmin',
            'members' => ['serviceAccount:' . $kbc1Credentials['client_email']],
        ]);
        $iamListingPolicy->setBindings($listingBindings);
        $exBqAnalyticHubClient->setIamPolicy($createdListing->getName(), $iamListingPolicy);

        // Use exBQ's SA as a proxy for KBC2's SA
        $exBqCredentialsArr = CredentialsHelper::getCredentialsArray($this->exBqCredentials);
        $kbc2ServiceAccountEmail = $exBqCredentialsArr['client_email'];

        // KBC1 calls LinkBucketHandler to link the external bucket into KBC1's project
        $handler = new LinkBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($kbc1ProjectId)
            ->setTargetBucketId($linkedBucketId)
            ->setSourceShareRoleName($createdListing->getName());

        $meta = new Any();
        $meta->pack(
            (new LinkBucketCommand\LinkBucketBigqueryMeta())
                ->setTargetServiceAccountEmail($kbc2ServiceAccountEmail),
        );
        $command->setMeta($meta);

        /** @var LinkedBucketResponse $result */
        $result = $handler(
            $this->kbc1Credentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(LinkedBucketResponse::class, $result);
        $this->assertSame($linkedDatasetName, $result->getLinkedBucketObjectName());

        // Verify the linked dataset was created in KBC1's project
        $this->assertTrue($kbc1BqClient->dataset($linkedDatasetName)->exists());

        // Verify KBC2's SA now has roles/analyticshub.subscriber on the listing
        $updatedListingPolicy = $exBqAnalyticHubClient->getIamPolicy($createdListing->getName());
        $subscriberGranted = false;
        /** @var Binding $binding */
        foreach ($updatedListingPolicy->getBindings() as $binding) {
            if ($binding->getRole() === 'roles/analyticshub.subscriber') {
                foreach ($binding->getMembers() as $member) {
                    if ($member === 'serviceAccount:' . $kbc2ServiceAccountEmail) {
                        $subscriberGranted = true;
                        break 2;
                    }
                }
            }
        }
        $this->assertTrue(
            $subscriberGranted,
            sprintf(
                "KBC2's SA '%s' should have roles/analyticshub.subscriber on the listing",
                $kbc2ServiceAccountEmail,
            ),
        );
    }

    /**
     * @return array{DataExchange, Listing}
     */
    private function prepareExternalBucketForRegistration(
        AnalyticsHubServiceClient $externalAnalyticHubClient,
        GenericBackendCredentials $externalProjectCredentials,
        string $bucketDatabaseName,
        string $location = BaseCase::DEFAULT_LOCATION,
    ): array {
        $externalCredentials = CredentialsHelper::getCredentialsArray($externalProjectCredentials);
        $externalProjectStringId = $externalCredentials['project_id'];

        $dataExchangeId = str_replace('-', '_', $externalProjectStringId) . '_external';
        $formattedParent = $externalAnalyticHubClient->locationName($externalProjectStringId, $location);

        $dataExchange = new DataExchange();
        $dataExchange->setDisplayName($dataExchangeId);

        try {
            $dataExchangeName = AnalyticsHubServiceClient::dataExchangeName(
                $externalProjectStringId,
                $location,
                $dataExchangeId,
            );
            $dataExchange = $externalAnalyticHubClient->getDataExchange($dataExchangeName);
            $externalAnalyticHubClient->deleteDataExchange($dataExchange->getName());
        } catch (Throwable) {
            // ignore
        }

        $dataExchange = $externalAnalyticHubClient->createDataExchange(
            $formattedParent,
            $dataExchangeId,
            $dataExchange,
        );

        $listingId = str_replace('-', '_', $externalProjectStringId) . '_listing';
        $lst = new BigQueryDatasetSource([
            'dataset' => sprintf(
                'projects/%s/datasets/%s',
                $externalProjectStringId,
                $bucketDatabaseName,
            ),
        ]);
        $listing = new Listing();
        $listing->setBigqueryDataset($lst);
        $listing->setDisplayName($listingId);

        $createdListing = $externalAnalyticHubClient->createListing($dataExchange->getName(), $listingId, $listing);
        return [$dataExchange, $createdListing];
    }
}
