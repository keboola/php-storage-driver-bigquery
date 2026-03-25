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
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\GrantBucketAccessToReadOnlyRoleHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Refresh\BigQueryExternalBucketSharingStatusHandler;
use Keboola\StorageDriver\Command\Bucket\BigQueryExternalBucketSharingStatusCommand;
use Keboola\StorageDriver\Command\Bucket\BigQueryExternalBucketSharingStatusResponse;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleCommand;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Throwable;

class BigQueryExternalBucketSharingStatusTest extends BaseCase
{
    private GenericBackendCredentials $mainProjectCredentials;

    private GenericBackendCredentials $externalProjectCredentials;

    private CreateBucketResponse $bucketResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->mainProjectCredentials = $this->projects[0][0];
        $this->externalProjectCredentials = $this->projects[1][0];
        $this->bucketResponse = $this->createTestBucket($this->projects[1][0]);
    }

    public function testGetSharingStatusForLinkedDataset(): void
    {
        $externalBucketName = $this->bucketResponse->getCreateBucketObjectName();
        $externalTableName = md5($this->name()) . '_Test_table';
        $this->createTestTable($this->externalProjectCredentials, $externalBucketName, $externalTableName);

        // Prepare exchange and listing in the external project
        $externalAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->externalProjectCredentials);
        $externalCredentials = CredentialsHelper::getCredentialsArray($this->externalProjectCredentials);
        $externalProjectStringId = $externalCredentials['project_id'];

        $dataExchangeId = str_replace('-', '_', $externalProjectStringId) . '_external_status';
        $formattedParent = $externalAnalyticHubClient->locationName(
            $externalProjectStringId,
            BaseCase::DEFAULT_LOCATION,
        );

        // Clean up any leftover exchanger from previous run
        try {
            $dataExchangeName = AnalyticsHubServiceClient::dataExchangeName(
                $externalProjectStringId,
                BaseCase::DEFAULT_LOCATION,
                $dataExchangeId,
            );
            $existingExchange = $externalAnalyticHubClient->getDataExchange($dataExchangeName);
            $externalAnalyticHubClient->deleteDataExchange($existingExchange->getName());
        } catch (Throwable) {
            // ignore
        }

        $dataExchange = new DataExchange();
        $dataExchange->setDisplayName($dataExchangeId);
        $dataExchange = $externalAnalyticHubClient->createDataExchange(
            $formattedParent,
            $dataExchangeId,
            $dataExchange,
        );

        $listingId = str_replace('-', '_', $externalProjectStringId) . '_listing_status';
        $lst = new BigQueryDatasetSource([
            'dataset' => sprintf('projects/%s/datasets/%s', $externalProjectStringId, $externalBucketName),
        ]);
        $listing = new Listing();
        $listing->setBigqueryDataset($lst);
        $listing->setDisplayName($listingId);
        $createdListing = $externalAnalyticHubClient->createListing($dataExchange->getName(), $listingId, $listing);

        // Grant subscriber access to main project SA
        $mainCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);
        $iamExchangerPolicy = $externalAnalyticHubClient->getIamPolicy($dataExchange->getName());
        $bindings = $iamExchangerPolicy->getBindings();
        $bindings[] = new Binding([
            'role' => 'roles/analyticshub.subscriber',
            'members' => ['serviceAccount:' . $mainCredentials['client_email']],
        ]);
        $iamExchangerPolicy->setBindings($bindings);
        $externalAnalyticHubClient->setIamPolicy($dataExchange->getName(), $iamExchangerPolicy);

        // Grant listingAdmin to main project SA so sharing is allowed
        $iamListingPolicy = $externalAnalyticHubClient->getIamPolicy($createdListing->getName());
        $listingBindings = $iamListingPolicy->getBindings();
        $listingBindings[] = new Binding([
            'role' => 'roles/analyticshub.listingAdmin',
            'members' => ['serviceAccount:' . $mainCredentials['client_email']],
        ]);
        $iamListingPolicy->setBindings($listingBindings);
        $externalAnalyticHubClient->setIamPolicy($createdListing->getName(), $iamListingPolicy);

        // Subscribe via GrantBucketAccessToReadOnlyRoleHandler to create the linked dataset
        /** @var array<string, string> $parsedName */
        $parsedName = AnalyticsHubServiceClient::parseName($createdListing->getName());

        $grantHandler = new GrantBucketAccessToReadOnlyRoleHandler($this->clientManager);
        $grantHandler->setInternalLogger($this->log);
        $grantCommand = (new GrantBucketAccessToReadOnlyRoleCommand())
            ->setPath([
                $parsedName['project'],
                $parsedName['location'],
                $parsedName['data_exchange'],
                $parsedName['listing'],
            ])
            ->setDestinationObjectName('test_ext_status_reg')
            ->setBranchId('123')
            ->setStackPrefix($this->getStackPrefix());
        $meta = new Any();
        $meta->pack((new GrantBucketAccessToReadOnlyRoleCommand\GrantBucketAccessToReadOnlyRoleBigqueryMeta()));
        $grantCommand->setMeta($meta);

        // Cleanup destination dataset if it exists from a previous run
        $mainBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->mainProjectCredentials);
        try {
            $linkedDataset = $mainBqClient->dataset('123_test_ext_status');
            if ($linkedDataset->exists()) {
                $linkedDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        /** @var GrantBucketAccessToReadOnlyRoleResponse $grantResult */
        $grantResult = $grantHandler(
            $this->mainProjectCredentials,
            $grantCommand,
            [],
            new RuntimeOptions(),
        );

        $linkedDatasetName = $grantResult->getCreateBucketObjectName();
        $this->assertNotEmpty($linkedDatasetName);

        // Now call GetBigQueryExternalBucketSharingStatusHandler
        $handler = new BigQueryExternalBucketSharingStatusHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = (new BigQueryExternalBucketSharingStatusCommand())
            ->setDestinationDatasetName($linkedDatasetName);

        /** @var BigQueryExternalBucketSharingStatusResponse $result */
        $result = $handler(
            $this->mainProjectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertSame($createdListing->getName(), $result->getSourceListing());
        $this->assertTrue($result->getSourceListingSharingAllowed());
    }

    public function testGetSharingStatusSharingNotAllowed(): void
    {
        $externalBucketName = $this->bucketResponse->getCreateBucketObjectName();
        $externalTableName = md5($this->name()) . '_Test_table';
        $this->createTestTable($this->externalProjectCredentials, $externalBucketName, $externalTableName);

        $externalAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->externalProjectCredentials);
        $externalCredentials = CredentialsHelper::getCredentialsArray($this->externalProjectCredentials);
        $externalProjectStringId = $externalCredentials['project_id'];

        $dataExchangeId = str_replace('-', '_', $externalProjectStringId) . '_external_no_share';
        $formattedParent = $externalAnalyticHubClient->locationName(
            $externalProjectStringId,
            BaseCase::DEFAULT_LOCATION,
        );

        // Clean up any leftover exchanger
        try {
            $dataExchangeName = AnalyticsHubServiceClient::dataExchangeName(
                $externalProjectStringId,
                BaseCase::DEFAULT_LOCATION,
                $dataExchangeId,
            );
            $existingExchange = $externalAnalyticHubClient->getDataExchange($dataExchangeName);
            $externalAnalyticHubClient->deleteDataExchange($existingExchange->getName());
        } catch (Throwable) {
            // ignore
        }

        $dataExchange = new DataExchange();
        $dataExchange->setDisplayName($dataExchangeId);
        $dataExchange = $externalAnalyticHubClient->createDataExchange(
            $formattedParent,
            $dataExchangeId,
            $dataExchange,
        );

        $listingId = str_replace('-', '_', $externalProjectStringId) . '_listing_no_share';
        $lst = new BigQueryDatasetSource([
            'dataset' => sprintf('projects/%s/datasets/%s', $externalProjectStringId, $externalBucketName),
        ]);
        $listing = new Listing();
        $listing->setBigqueryDataset($lst);
        $listing->setDisplayName($listingId);
        $createdListing = $externalAnalyticHubClient->createListing($dataExchange->getName(), $listingId, $listing);

        // Only grant subscriber — no listingAdmin, so sharing should be false
        $mainCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);
        $iamExchangerPolicy = $externalAnalyticHubClient->getIamPolicy($dataExchange->getName());
        $bindings = $iamExchangerPolicy->getBindings();
        $bindings[] = new Binding([
            'role' => 'roles/analyticshub.subscriber',
            'members' => ['serviceAccount:' . $mainCredentials['client_email']],
        ]);
        $iamExchangerPolicy->setBindings($bindings);
        $externalAnalyticHubClient->setIamPolicy($dataExchange->getName(), $iamExchangerPolicy);

        /** @var array<string, string> $parsedName */
        $parsedName = AnalyticsHubServiceClient::parseName($createdListing->getName());

        $grantHandler = new GrantBucketAccessToReadOnlyRoleHandler($this->clientManager);
        $grantHandler->setInternalLogger($this->log);
        $grantCommand = (new GrantBucketAccessToReadOnlyRoleCommand())
            ->setPath([
                $parsedName['project'],
                $parsedName['location'],
                $parsedName['data_exchange'],
                $parsedName['listing'],
            ])
            ->setDestinationObjectName('test_ext_no_share')
            ->setBranchId('123')
            ->setStackPrefix($this->getStackPrefix());
        $meta = new Any();
        $meta->pack((new GrantBucketAccessToReadOnlyRoleCommand\GrantBucketAccessToReadOnlyRoleBigqueryMeta()));
        $grantCommand->setMeta($meta);

        $mainBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->mainProjectCredentials);
        try {
            $linkedDataset = $mainBqClient->dataset('123_test_ext_no_share');
            if ($linkedDataset->exists()) {
                $linkedDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        /** @var GrantBucketAccessToReadOnlyRoleResponse $grantResult */
        $grantResult = $grantHandler(
            $this->mainProjectCredentials,
            $grantCommand,
            [],
            new RuntimeOptions(),
        );

        $linkedDatasetName = $grantResult->getCreateBucketObjectName();

        $handler = new BigQueryExternalBucketSharingStatusHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = (new BigQueryExternalBucketSharingStatusCommand())
            ->setDestinationDatasetName($linkedDatasetName);

        /** @var BigQueryExternalBucketSharingStatusResponse $result */
        $result = $handler(
            $this->mainProjectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertSame($createdListing->getName(), $result->getSourceListing());
        $this->assertFalse($result->getSourceListingSharingAllowed());
    }
}
