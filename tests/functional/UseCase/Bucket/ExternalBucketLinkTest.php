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
    private GenericBackendCredentials $mainProjectCredentials;

    private GenericBackendCredentials $ebProducerCredentials;

    private GenericBackendCredentials $linkedCredentials;

    protected function setUp(): void
    {
        parent::setUp();
        // KBC1 = projects[0]: the project that registered the external bucket and wants to link it
        $this->mainProjectCredentials = $this->projects[0][0];

        // project that supposes to link the bucket (KBC2) by sharing from KBC1
        $this->linkedCredentials = $this->projects[1][0];

        // exBQ = projects[2]: the external project that owns the Analytics Hub listing
        $this->ebProducerCredentials = $this->projects[2][0];
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
        $exBqBucketResponse = $this->createTestBucket($this->ebProducerCredentials);
        $exBqDatasetName = $exBqBucketResponse->getCreateBucketObjectName();

        // project where the EB will be registered to and where it is shared from. AKA KBC1
        $mainProjectCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);

        // project where the EB will be linked to. AKA KBC2
        $linkingProjectCredentials = CredentialsHelper::getCredentialsArray($this->linkedCredentials);
        $linkingProjectId = $linkingProjectCredentials['project_id'];

        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $linkedBucketId = $this->getTestHash() . 'in.c-Linked';
        $linkedDatasetName = $nameGenerator->createObjectNameForBucketInProject($linkedBucketId, null);

        $mainProjectBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->mainProjectCredentials);
        $linkingProjectBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->linkedCredentials);

        // cleanup at beginning - delete linked dataset if it exists from previous failed run
        try {
            if ($mainProjectBqClient->dataset($linkedDatasetName)->exists()) {
                $mainProjectBqClient->dataset($linkedDatasetName)->delete(['deleteContents' => true]);
            }
            if ($linkingProjectBqClient->dataset($linkedDatasetName)->exists()) {
                $linkingProjectBqClient->dataset($linkedDatasetName)->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        // 1. register the EB - producer creates an exchange and listing pointing to the external bucket,
        // and set KBC1 as subscriber and listing admin so KBC1 can share and link the bucket

        // exBQ creates exchange + listing pointing to exBQ's dataset
        $exBqAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->ebProducerCredentials);
        [$dataExchange, $createdListing] = $this->prepareExternalBucketForRegistration(
            $exBqAnalyticHubClient,
            $this->ebProducerCredentials,
            $exBqDatasetName,
        );

        // 1.2 exBQ grants roles/analyticshub.subscriber on the exchange to KBC1's SA
        // so KBC1 can call subscribeListing
        $iamExchangePolicy = $exBqAnalyticHubClient->getIamPolicy($dataExchange->getName());
        $exchangeBindings = $iamExchangePolicy->getBindings();
        $exchangeBindings[] = new Binding([
            'role' => 'roles/analyticshub.subscriber',
            'members' => ['serviceAccount:' . $mainProjectCredentials['client_email']],
        ]);
        $iamExchangePolicy->setBindings($exchangeBindings);
        $exBqAnalyticHubClient->setIamPolicy($dataExchange->getName(), $iamExchangePolicy);

        // 1.3 exBQ grants roles/analyticshub.listingAdmin on the listing to KBC1's SA
        // so KBC1 can grant subscriber access to KBC2 (get/set IAM policy on listing)
        $iamListingPolicy = $exBqAnalyticHubClient->getIamPolicy($createdListing->getName());
        $listingBindings = $iamListingPolicy->getBindings();
        $listingBindings[] = new Binding([
            'role' => 'roles/analyticshub.listingAdmin',
            'members' => ['serviceAccount:' . $mainProjectCredentials['client_email']],
        ]);
        $iamListingPolicy->setBindings($listingBindings);
        $exBqAnalyticHubClient->setIamPolicy($createdListing->getName(), $iamListingPolicy);

        /*
         * 2.0. link bucket in KBC2 (linking), passing the listing reference and target SA email in meta
         * to signal LinkBucketHandler it's an external listing
         * sharing bucket from KBC1 is not needed because it is in exchanger from exEB already, and it will use this one
         * We need to do is set KBC2 as a subscriber. And we should be able do that, beacuse we have the listingAdmin
         */
        $linkinProjectServiceAccountEmail = $linkingProjectCredentials['client_email'];

        // KBC1 calls LinkBucketHandler to link the external bucket into KBC1's project
        $handler = new LinkBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($linkingProjectId)
            ->setTargetBucketId($linkedBucketId)
            ->setSourceShareRoleName($createdListing->getName());

        $meta = new Any();
        $meta->pack(
            (new LinkBucketCommand\LinkBucketBigqueryMeta())
                ->setTargetServiceAccountEmail($linkinProjectServiceAccountEmail),
        );
        $command->setMeta($meta);

        /** @var LinkedBucketResponse $result */
        $result = $handler(
            // using the root credentials of both projects
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(LinkedBucketResponse::class, $result);
        $this->assertSame($linkedDatasetName, $result->getLinkedBucketObjectName());

        // Verify the linked dataset was created in KBC1's project
        $this->assertTrue($linkingProjectBqClient->dataset($linkedDatasetName)->exists());

        // Verify KBC2's SA now has roles/analyticshub.subscriber on the listing
        $updatedListingPolicy = $exBqAnalyticHubClient->getIamPolicy($createdListing->getName());
        $subscriberGranted = false;
        /** @var Binding $binding */
        foreach ($updatedListingPolicy->getBindings() as $binding) {
            if ($binding->getRole() === 'roles/analyticshub.subscriber') {
                foreach ($binding->getMembers() as $member) {
                    if ($member === 'serviceAccount:' . $linkinProjectServiceAccountEmail) {
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
                $linkinProjectServiceAccountEmail,
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
    ): array {
        $externalCredentials = CredentialsHelper::getCredentialsArray($externalProjectCredentials);
        $externalProjectStringId = $externalCredentials['project_id'];

        $dataExchangeId = str_replace('-', '_', $externalProjectStringId) . '_external';
        $formattedParent = $externalAnalyticHubClient->locationName(
            $externalProjectStringId,
            BaseCase::DEFAULT_LOCATION,
        );

        $dataExchange = new DataExchange();
        $dataExchange->setDisplayName($dataExchangeId);

        try {
            $dataExchangeName = AnalyticsHubServiceClient::dataExchangeName(
                $externalProjectStringId,
                BaseCase::DEFAULT_LOCATION,
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
