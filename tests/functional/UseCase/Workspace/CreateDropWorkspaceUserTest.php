<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\Core\Exception\ServiceException;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Service\CloudResourceManager\GetIamPolicyRequest;
use Google\Service\Exception as GoogleServiceException;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\Helper;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\CreateUser\CreateWorkspaceUserHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\DropUser\DropWorkspaceUserHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceUserCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceUserResponse;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceUserCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

/**
 * @group sync
 */
class CreateDropWorkspaceUserTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];
        $this->projectResponse = $this->projects[0][1];
    }

    public function testCreateDropWorkspaceUser(): void
    {
        // Create two workspaces so we can test cross-workspace isolation
        [
            $wsCredentials1,
            $wsResponse1,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);

        [
            ,
            $wsResponse2,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);

        $this->assertInstanceOf(GenericBackendCredentials::class, $wsCredentials1);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $wsResponse1);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $projectCredentials = CredentialsHelper::getCredentialsArray($this->projectCredentials);
        $projectId = $projectCredentials['project_id'];

        // CREATE workspace user - new SA with access to existing workspace dataset
        $handler = new CreateWorkspaceUserHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        // Use a short workspaceId for SA name generation (must be ≤30 chars total)
        $shortWsId = 'WS' . self::getRand();
        $command = (new CreateWorkspaceUserCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setWorkspaceId($shortWsId)
            ->setWorkspaceObjectName($wsResponse1->getWorkspaceObjectName())
            ->setProjectReadOnlyRoleName($this->projectResponse->getProjectReadOnlyRoleName());

        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(CreateWorkspaceUserResponse::class, $response);
        $this->assertNotEmpty($response->getWorkspaceUserName());
        $this->assertNotEmpty($response->getWorkspacePassword());

        // Parse the new user's credentials
        /** @var array<string, string> $newUserKeyData */
        $newUserKeyData = json_decode($response->getWorkspaceUserName(), true, 512, JSON_THROW_ON_ERROR);
        $newUserEmail = $newUserKeyData['client_email'];

        // Verify the new service account exists
        $iamService = $this->clientManager->getIamClient($this->projectCredentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $newServiceAcc = $serviceAccountsService->get(
            sprintf('projects/%s/serviceAccounts/%s', $projectId, $newUserEmail),
        );
        $this->assertNotNull($newServiceAcc);

        // Verify the new user has OWNER access on the workspace dataset
        /** @var array<string, mixed> $workspaceDataset */
        $workspaceDataset = $bqClient->dataset($wsResponse1->getWorkspaceObjectName())->info();
        /** @var list<array<string, mixed>> $accessList */
        $accessList = $workspaceDataset['access'] ?? [];
        $newUserHasAccess = false;
        foreach ($accessList as $accessEntry) {
            if (isset($accessEntry['userByEmail']) && $accessEntry['userByEmail'] === $newUserEmail) {
                $this->assertSame('OWNER', $accessEntry['role']);
                $newUserHasAccess = true;
            }
        }
        $this->assertTrue($newUserHasAccess, 'New workspace user should have OWNER access on workspace dataset');

        // Verify the new user has proper IAM bindings
        Helper::assertServiceAccountBindings(
            $this->clientManager->getCloudResourceManager($this->projectCredentials),
            'projects/' . $projectId,
            $newUserEmail,
            $this->log,
        );

        // Build credentials for the new workspace user
        $meta = new Any();
        $meta->pack(
            (new GenericBackendCredentials\BigQueryCredentialsMeta())
                ->setRegion(self::DEFAULT_LOCATION),
        );
        $newUserCredentials = (new GenericBackendCredentials())
            ->setHost($this->projectCredentials->getHost())
            ->setPrincipal($response->getWorkspaceUserName())
            ->setSecret($response->getWorkspacePassword())
            ->setPort($this->projectCredentials->getPort());
        $newUserCredentials->setMeta($meta);

        $newUserBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $newUserCredentials);

        // =====================================================================
        // Permission tests: verify the new workspace user has correct BQ permissions
        // =====================================================================

        // 1. New user CANNOT create datasets (buckets)
        $uniqueBucketId = 'ws_user_perm_test_' . uniqid();
        $bucketHandler = new CreateBucketHandler($this->clientManager);
        $bucketHandler->setInternalLogger($this->log);
        $bucketCommand = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setBucketId($uniqueBucketId);
        $bucketMeta = new Any();
        $bucketMeta->pack(new CreateBucketCommand\CreateBucketBigqueryMeta());
        $bucketCommand->setMeta($bucketMeta);
        try {
            $bucketHandler(
                $newUserCredentials,
                $bucketCommand,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('Workspace user should not be able to create datasets');
        } catch (ServiceException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertStringContainsString(
                'User does not have bigquery.datasets.create permission in project',
                $e->getMessage(),
            );
        }

        // 2. Create a bucket with data for read-only permission tests
        $tableName = 'testTable';
        $bucketDatasetName = $this->createTestBucket($this->projectCredentials);
        $this->createNonEmptyTableInBucket($bucketDatasetName->getCreateBucketObjectName(), $tableName);

        // 3. New user CANNOT write to read-only bucket tables
        $insertGroups = [
            [
                'columns' => '`id`',
                'rows' => ['4', '5', '6'],
            ],
        ];
        try {
            $this->fillTableWithData(
                $newUserCredentials,
                $bucketDatasetName->getCreateBucketObjectName(),
                $tableName,
                $insertGroups,
            );
            $this->fail('Workspace user should not be able to write to read-only bucket tables');
        } catch (ServiceException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertStringContainsString('Access Denied: ', $e->getMessage());
        }

        // 4. New user CAN read from bucket tables (read-only access)
        $result = $newUserBqClient->runQuery($newUserBqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        )));
        $this->assertCount(3, $result);

        // 5. New user CAN create tables in own workspace
        $newUserBqClient->runQuery($newUserBqClient->query(sprintf(
            'CREATE TABLE %s.`test_ws_user_table` (`id` INTEGER)',
            BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
        )));

        // 6. New user CAN create views in own workspace
        $newUserBqClient->runQuery($newUserBqClient->query(sprintf(
            'CREATE VIEW %s.`test_ws_user_view` AS SELECT `id` FROM %s.`test_ws_user_table`',
            BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
            BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
        )));

        // 7. New user CANNOT see other workspace datasets
        $actualDatasets = [];
        /** @var Dataset $dataset */
        foreach ($newUserBqClient->datasets() as $dataset) {
            /** @var array{datasetReference: array{datasetId: string}} $datasetInfo */
            $datasetInfo = $dataset->info();
            $actualDatasets[] = $datasetInfo['datasetReference']['datasetId'];
        }
        $this->assertNotContains(
            $wsResponse2->getWorkspaceObjectName(),
            $actualDatasets,
            'Workspace user should not see other workspace datasets',
        );

        // 8. New user CANNOT read from other workspaces
        // First create a table in workspace2 using project credentials
        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE TABLE %s.`other_ws_table` (`id` INTEGER)',
            BigqueryQuote::quoteSingleIdentifier($wsResponse2->getWorkspaceObjectName()),
        )));
        try {
            $newUserBqClient->runQuery($newUserBqClient->query(sprintf(
                'SELECT * FROM %s.`other_ws_table`',
                BigqueryQuote::quoteSingleIdentifier($wsResponse2->getWorkspaceObjectName()),
            )));
            $this->fail('Workspace user should not be able to read from other workspaces');
        } catch (ServiceException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertStringContainsString(
                'User does not have permission to query table',
                $e->getMessage(),
            );
        }

        // 9. New user CANNOT write into other workspaces
        try {
            $newUserBqClient->runQuery($newUserBqClient->query(sprintf(
                'CREATE TABLE %s.`unauthorized_table` (`id` INTEGER)',
                BigqueryQuote::quoteSingleIdentifier($wsResponse2->getWorkspaceObjectName()),
            )));
            $this->fail('Workspace user should not be able to write to other workspaces');
        } catch (ServiceException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertStringContainsString(
                'Permission bigquery.tables.create denied on dataset',
                $e->getMessage(),
            );
        }

        // Clean up workspace tables
        $newUserBqClient->runQuery($newUserBqClient->query(sprintf(
            'DROP VIEW %s.`test_ws_user_view`',
            BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
        )));
        $newUserBqClient->runQuery($newUserBqClient->query(sprintf(
            'DROP TABLE %s.`test_ws_user_table`',
            BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
        )));

        // =====================================================================
        // DROP workspace user and verify cleanup
        // =====================================================================

        $dropHandler = new DropWorkspaceUserHandler($this->clientManager);
        $dropHandler->setInternalLogger($this->log);

        $dropCommand = (new DropWorkspaceUserCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName());

        $dropResponse = $dropHandler(
            $this->projectCredentials,
            $dropCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertNull($dropResponse);

        // Verify the service account has been deleted
        try {
            $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
                if ($e->getMessage() === 'Service account should be deleted.') {
                    return true;
                }
                return false;
            });
            $proxy = new RetryProxy($retryPolicy, new ExponentialBackOffPolicy());
            $proxy->call(function () use ($serviceAccountsService, $projectId, $newUserEmail): void {
                $serviceAccountsService->get(
                    sprintf('projects/%s/serviceAccounts/%s', $projectId, $newUserEmail),
                );
                $this->fail('Service account should be deleted.');
            });
        } catch (GoogleServiceException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertStringContainsString('.iam.gserviceaccount.com does not exist.', $e->getMessage());
        }

        // Verify IAM policies have been cleaned up
        $cloudResourceManager = $this->clientManager->getCloudResourceManager($this->projectCredentials);
        /** @var \Google\Service\CloudResourceManager\Resource\Projects $projects */
        $projects = $cloudResourceManager->projects;
        /** @var \Google\Service\CloudResourceManager\Policy $actualPolicy */
        $actualPolicy = $projects->getIamPolicy(
            'projects/' . $projectId,
            (new GetIamPolicyRequest()),
            [],
        );

        /** @var \Google\Service\CloudResourceManager\Binding $binding */
        foreach ($actualPolicy->getBindings() as $binding) {
            /** @var string[] $members */
            $members = $binding->getMembers();
            $this->assertNotContains(
                'serviceAccount:' . $newUserEmail,
                $members,
                sprintf(
                    'Service account %s should be removed from IAM binding for role %s',
                    $newUserEmail,
                    $binding->getRole(),
                ),
            );
        }
    }

    private function createNonEmptyTableInBucket(string $bucketDatasetName, string $tableName): void
    {
        $tableStructure = [
            'columns' => [
                'col1' => [
                    'type' => Bigquery::TYPE_STRING,
                    'nullable' => false,
                    'length' => '',
                    'default' => '\'\'',
                ],
                'col2' => [
                    'type' => Bigquery::TYPE_STRING,
                    'nullable' => false,
                    'length' => '',
                    'default' => '\'\'',
                ],
                'col3' => [
                    'type' => Bigquery::TYPE_STRING,
                    'nullable' => false,
                    'length' => '',
                    'default' => '\'\'',
                ],
                '_timestamp' => [
                    'type' => Bigquery::TYPE_TIMESTAMP,
                    'nullable' => false,
                    'length' => '',
                    'default' => '\'\'',
                ],
            ],
            'primaryKeysNames' => [],
        ];

        $this->createTable($this->projectCredentials, $bucketDatasetName, $tableName, $tableStructure);

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $cmd->setFileProvider(FileProvider::GCS);
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'col1';
        $columns[] = 'col2';
        $columns[] = 'col3';
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SINGLE_FILE)
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::NONE),
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath('import')
                ->setFileName('a_b_c-3row.csv'),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($tableName),
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setTimestampColumn('_timestamp'),
        );

        $importHandler = new ImportTableFromFileHandler($this->clientManager);
        $importHandler->setInternalLogger($this->log);
        $importHandler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }
}
