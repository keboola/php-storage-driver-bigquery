<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DataExchange;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing\BigQueryDatasetSource;
use Google\Cloud\Core\Exception\NotFoundException;
use Google\Cloud\Iam\V1\Binding;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\GrantBucketAccessToReadOnlyRoleHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop\RevokeBucketAccessFromReadOnlyRoleHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleCommand;
use Keboola\StorageDriver\Command\Bucket\RevokeBucketAccessFromReadOnlyRoleCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Throwable;

class GrantRevokeBucketAccessToReadOnlyRoleTest extends BaseCase
{
    private GenericBackendCredentials $mainProjectCredentials;
    private GenericBackendCredentials $externalProjectCredentials;
    private CreateBucketResponse $bucketResponse;

    private const PROJ_SUFFIX = '-e';

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();
        [$credentials1, $response1] = $this->createTestProject();
        $this->mainProjectCredentials = $credentials1;
        $this->projectSuffix = self::PROJ_SUFFIX;

        [$credentials2, $response2] = $this->createTestProject();
        $this->externalProjectCredentials = $credentials2;
        $bucketResponse = $this->createTestBucket($credentials2);
        $this->bucketResponse = $bucketResponse;
    }

    public function testRegisterBucket(): void
    {
        // prepare test external table
        $externalBucketName = $this->bucketResponse->getCreateBucketObjectName();
        $externalTableName = md5($this->getName()) . '_Test_table';
        $this->prepareTestTable($externalBucketName, $externalTableName);

        // validate initial state of projects
        $mainBqClient = $this->clientManager->getBigQueryClient($this->mainProjectCredentials);
        $externalBqClient = $this->clientManager->getBigQueryClient($this->externalProjectCredentials);

        $this->assertCount(0, $mainBqClient->datasets());
        $this->assertCount(1, $externalBqClient->datasets());

        // this part simulate user who want to register ext bucket
        // 1. and 2. will be done in one step, but we need to test it can't be registered before grant permission
        // 1. User which want to register external bucket create exchanged and listing
        $externalAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->externalProjectCredentials);
        [$dataExchange, $createdListing] = $this->prepareExternalBucketForRegistration(
            $externalAnalyticHubClient,
            $externalBucketName
        );

        try {
            $handler = new GrantBucketAccessToReadOnlyRoleHandler($this->clientManager);
            $command = (new GrantBucketAccessToReadOnlyRoleCommand())
                ->setBucketObjectName($createdListing->getName())
                ->setProjectReadOnlyRoleName('test_external');
            $handler(
                $this->mainProjectCredentials,
                $command,
                []
            );
            $this->fail('Should not be able to register bucket from another project, until user grant subscription.');
        } catch (Throwable $e) {
            $msg = sprintf(
                'Failed to register external bucket "test_external" permission denied for subscribe listing "%s"',
                $createdListing->getName()
            );
            $this->assertSame($msg, $e->getMessage());
        }

        // 2. Grant subscribe permission to external bucket to service account if destination project
        $this->grantMainProjectToRegisterExternalBucket($externalAnalyticHubClient, $dataExchange);

        // Now we should be able to link (register) external bucket
        $handler = new GrantBucketAccessToReadOnlyRoleHandler($this->clientManager);
        $command = (new GrantBucketAccessToReadOnlyRoleCommand())
            ->setBucketObjectName($createdListing->getName())
            ->setProjectReadOnlyRoleName('test_external');
        $handler(
            $this->mainProjectCredentials,
            $command,
            []
        );

        // Validate is bucket added
        $mainBqClient = $this->clientManager->getBigQueryClient($this->mainProjectCredentials);
        $this->assertCount(1, $mainBqClient->datasets());
        $registeredExternalBucketInMainProject = $mainBqClient->dataset('test_external');
        $registeredTables = $registeredExternalBucketInMainProject->tables();
        $this->assertCount(1, $registeredTables);

        // And I can get rows from external table
        $result = $mainBqClient->runQuery(
            $mainBqClient->query('SELECT * FROM `test_external`.`' . $externalTableName . '`')
        );
        $this->assertCount(3, $result);

        $this->assertEqualsCanonicalizing(
            [
                [
                    'id' => '1',
                    'name' => 'external',
                    'large' => 'data',
                ],
                [
                    'id' => '2',
                    'name' => 'data from',
                    'large' => 'external table',
                ],
                [
                    'id' => '3',
                    'name' => 'it works',
                    'large' => 'awesome !',
                ],
            ],
            iterator_to_array($result->rows())
        );

        // Add more data to test rows will be added in main project
        $insertGroups = [
            [
                'columns' => '`id`, `name`, `large`',
                'rows' => [
                    "4, 'more', 'data'",
                ],
            ],
        ];
        $this->fillTableWithData(
            $this->externalProjectCredentials,
            $externalBucketName,
            $externalTableName,
            $insertGroups
        );

        // And I can get rows from external table
        $result = $mainBqClient->runQuery(
            $mainBqClient->query('SELECT * FROM `test_external`.`' . $externalTableName . '`')
        );
        $this->assertCount(4, $result);

        $this->assertEqualsCanonicalizing(
            [
                [
                    'id' => '1',
                    'name' => 'external',
                    'large' => 'data',
                ],
                [
                    'id' => '2',
                    'name' => 'data from',
                    'large' => 'external table',
                ],
                [
                    'id' => '3',
                    'name' => 'it works',
                    'large' => 'awesome !',
                ],
                [
                    'id' => '4',
                    'name' => 'more',
                    'large' => 'data',
                ],
            ],
            iterator_to_array($result->rows())
        );

        $handler = new RevokeBucketAccessFromReadOnlyRoleHandler($this->clientManager);
        $command = (new RevokeBucketAccessFromReadOnlyRoleCommand())
            ->setBucketObjectName($createdListing->getName())
            ->setProjectReadOnlyRoleName('test_external');
        $handler(
            $this->mainProjectCredentials,
            $command,
            []
        );

        try {
            $mainBqClient->runQuery(
                $mainBqClient->query('SELECT * FROM `test_external`.`' . $externalTableName . '`')
            );
            $this->fail('Should not be able to get data from external table after revoke access.');
        } catch (NotFoundException $e) {
            $mainCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);
            $mainProjectStringId = $mainCredentials['project_id'];

            $this->assertSame(
                sprintf('Not found: Dataset %s:test_external was not found in location US', $mainProjectStringId),
                json_decode($e->getMessage())->error->message
            );
        }

        $this->assertCount(0, $mainBqClient->datasets());
        $this->assertCount(1, $externalBqClient->datasets());
    }

    private function prepareTestTable(string $bucketDatabaseName, string $externalTableName): void
    {
        $this->createTestTable($this->externalProjectCredentials, $bucketDatabaseName, $externalTableName);

        // FILL DATA
        $insertGroups = [
            [
                'columns' => '`id`, `name`, `large`',
                'rows' => [
                    "1, 'external', 'data'",
                    "2, 'data from', 'external table'",
                    "3, 'it works', 'awesome !'",
                ],
            ],
        ];
        $this->fillTableWithData(
            $this->externalProjectCredentials,
            $bucketDatabaseName,
            $externalTableName,
            $insertGroups
        );
    }

    /**
     * @return array{DataExchange, Listing}
     */
    private function prepareExternalBucketForRegistration(
        AnalyticsHubServiceClient $externalAnalyticHubClient,
        string $bucketDatabaseName
    ): array {
        $externalCredentials = CredentialsHelper::getCredentialsArray($this->externalProjectCredentials);
        $externalProjectStringId = $externalCredentials['project_id'];

        $dataExchangeId = str_replace('-', '_', $externalProjectStringId) . '_external';
        $location = GCPClientManager::DEFAULT_LOCATION;
        $formattedParent = $externalAnalyticHubClient->locationName($externalProjectStringId, $location);

        // 1.1 Create exchanger in source project
        $dataExchange = new DataExchange();
        $dataExchange->setDisplayName($dataExchangeId);
        $dataExchange = $externalAnalyticHubClient->createDataExchange(
            $formattedParent,
            $dataExchangeId,
            $dataExchange
        );

        $listingId = str_replace('-', '_', $externalCredentials['project_id']) . '_listing';
        $lst = new BigQueryDatasetSource([
            'dataset' => sprintf(
                'projects/%s/datasets/%s',
                $externalProjectStringId,
                $bucketDatabaseName
            ),
        ]);
        $listing = new Listing();
        $listing->setBigqueryDataset($lst);
        $listing->setDisplayName($listingId);

        // 1.2 Create listing for extern bucket
        $createdListing = $externalAnalyticHubClient->createListing($dataExchange->getName(), $listingId, $listing);
        return [$dataExchange, $createdListing];
    }

    private function grantMainProjectToRegisterExternalBucket(
        AnalyticsHubServiceClient $externalAnalyticHubClient,
        DataExchange $dataExchange
    ): void {
        $mainCredentials = CredentialsHelper::getCredentialsArray($this->mainProjectCredentials);
        $iamExchangerPolicy = $externalAnalyticHubClient->getIamPolicy($dataExchange->getName());
        $binding = $iamExchangerPolicy->getBindings();
        $binding[] = new Binding([
            'role' => 'roles/analyticshub.subscriber',
            'members' => ['serviceAccount:' . $mainCredentials['client_email']],
        ]);
        $iamExchangerPolicy->setBindings($binding);
        $externalAnalyticHubClient->setIamPolicy($dataExchange->getName(), $iamExchangerPolicy);
    }
}
