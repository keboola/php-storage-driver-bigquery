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
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Drop\DropWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\BackOff\FixedBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Throwable;

/**
 * @group sync
 */
class CreateDropWorkspaceTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];
        $this->projectResponse = $this->projects[0][1];
    }

    public function testCreateDropWorkspace(): void
    {
        // CREATE
        [
            $wsCredentials1,
            $wsResponse1,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse, $this->projects[0][2]);

        [
            $wsCredentials2,
            $wsResponse2,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse, $this->projects[0][2]);

        $this->assertInstanceOf(GenericBackendCredentials::class, $wsCredentials1);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $wsResponse1);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $wsKeyData = CredentialsHelper::getCredentialsArray($wsCredentials1);
        $projectId = $wsKeyData['project_id'];
        $wsServiceAccEmail = $wsKeyData['client_email'];

        // check objects created
        $iamService = $this->clientManager->getIamClient($this->projectCredentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $wsServiceAcc = $serviceAccountsService->get(
            sprintf('projects/%s/serviceAccounts/%s', $projectId, $wsServiceAccEmail),
        );
        $this->assertNotNull($wsServiceAcc);

        $ws1BqClient = $this->clientManager->getBigQueryClient($this->testRunId, $wsCredentials1);

        /** @var array<string, string> $datasets */
        $datasets = $bqClient->executeQuery($bqClient->query(sprintf(
        /** @lang BigQuery */
            'SELECT `schema_name` FROM %s.INFORMATION_SCHEMA.SCHEMATA WHERE `schema_name` IN (%s,%s) ;',
            BigqueryQuote::quoteSingleIdentifier($projectId),
            BigqueryQuote::quote(strtoupper($wsResponse1->getWorkspaceObjectName())),
            BigqueryQuote::quote(strtoupper($wsResponse2->getWorkspaceObjectName())),
        )));

        // two workspace datasets
        $this->assertCount(2, $datasets);

        // test ws service acc is owner of ws dataset
        $workspaceDataset = $bqClient->dataset($wsResponse1->getWorkspaceObjectName())->info();
        $this->assertNotNull($workspaceDataset);
        $this->assertCount(1, $workspaceDataset['access']);
        $this->assertSame('OWNER', $workspaceDataset['access'][0]['role']);
        $this->assertSame($wsServiceAccEmail, $workspaceDataset['access'][0]['userByEmail']);

        Helper::assertServiceAccountBindings(
            $this->clientManager->getCloudResourceManager($this->projectCredentials),
            $projectId,
            $wsServiceAccEmail,
            $this->log,
        );

        try {
            $this->createTestBucket($wsCredentials1, $this->projects[0][2]);
            $this->fail('The workspace user should not have the right to create a new dataset.');
        } catch (ServiceException $exception) {
            $this->assertSame(403, $exception->getCode());
            $this->assertStringContainsString(
                'User does not have bigquery.datasets.create permission in project',
                $exception->getMessage(),
            );
        }

        $tableName = 'testTable';
        $bucketDatasetName = $this->createTestBucket($this->projectCredentials, $this->projects[0][2]);
        $this->createNonEmptyTableInBucket($bucketDatasetName->getCreateBucketObjectName(), $tableName);

        // FILL DATA
        $insertGroups = [
            [
                'columns' => '`id`',
                'rows' => [
                    '4',
                    '5',
                    '6',
                ],
            ],
        ];

        try {
            $this->fillTableWithData(
                $wsCredentials1,
                $bucketDatasetName->getCreateBucketObjectName(),
                $tableName,
                $insertGroups,
            );
            $this->fail('Insert to read only table should failed');
        } catch (ServiceException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertStringContainsString('Access Denied: ', $e->getMessage());
        }

        $result = $ws1BqClient->executeQuery($ws1BqClient->query(sprintf(
            'SELECT * FROM %s.%s;',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        )));

        $this->assertCount(3, $result);
        // try to create table
        $ws1BqClient->executeQuery($ws1BqClient->query(sprintf(
            'CREATE TABLE %s.`testTable` (`id` INTEGER);',
            BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
        )));

        // try to create view
        $ws1BqClient->executeQuery($ws1BqClient->query(sprintf(
            'CREATE VIEW %s.`testView` AS '
            . 'SELECT `id` FROM %s.`testTable`;',
            BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
            BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
        )));

        $ws2BqClient = $this->clientManager->getBigQueryClient($this->testRunId, $wsCredentials2);
        // test WS2 can see only own WS dataset and bucket dataset via RO
        $actualDatasetsInWs2 = [];
        /** @var Dataset $dataset */
        foreach ($ws2BqClient->datasets() as $dataset) {
            $actualDatasetsInWs2[] = $dataset->info()['datasetReference']['datasetId'];
        }
        $this->assertNotContains($wsResponse1->getWorkspaceObjectName(), $actualDatasetsInWs2);

        // test WS2 can't read from other workspaces
        try {
            $ws2BqClient->executeQuery($ws2BqClient->query(sprintf(
                'SELECT * FROM %s.`testTable`;',
                BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
            )));
            $this->fail('Read from another workspace should fail');
        } catch (ServiceException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertStringContainsString('User does not have permission to query table', $e->getMessage());
        }

        // test WS2 can't write into other workspaces
        try {
            $ws2BqClient->executeQuery($ws2BqClient->query(sprintf(
                'CREATE TABLE %s.`testTable` (`id` INTEGER);',
                BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
            )));

            $this->fail('Write in another workspace should fail');
        } catch (ServiceException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertStringContainsString('Permission bigquery.tables.create denied on dataset', $e->getMessage());
        }

        // try to drop view
        $ws1BqClient->executeQuery($ws1BqClient->query(sprintf(
            'DROP VIEW %s.`testView`;',
            BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
        )));

        // try to drop table
        $ws1BqClient->executeQuery($ws1BqClient->query(sprintf(
            'DROP TABLE %s.`testTable`;',
            BigqueryQuote::quoteSingleIdentifier($wsResponse1->getWorkspaceObjectName()),
        )));

        // DROP
        $handler = new DropWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($wsResponse1->getWorkspaceUserName())
            ->setWorkspaceRoleName($wsResponse1->getWorkspaceRoleName())
            ->setWorkspaceObjectName($wsResponse1->getWorkspaceObjectName());

        $dropResponse = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertNull($dropResponse);

        try {
            $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
                if ($e->getMessage() === 'Service account should be deleted.') {
                    return true;
                }
                return false;
            });
            $proxy = new RetryProxy($retryPolicy, new ExponentialBackOffPolicy());
            $proxy->call(function () use ($serviceAccountsService, $projectId, $wsServiceAccEmail): void {
                // deleting can take a while before it shows up
                $serviceAccountsService->get(sprintf('projects/%s/serviceAccounts/%s', $projectId, $wsServiceAccEmail));
                $this->fail('Service account should be deleted.');
            });
        } catch (GoogleServiceException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertStringContainsString('.iam.gserviceaccount.com does not exist.', $e->getMessage());
        }

        $datasets = $bqClient->executeQuery(
            $bqClient->query(sprintf(
                'SELECT schema_name FROM %s.INFORMATION_SCHEMA.SCHEMATA WHERE `schema_name` = %s;',
                BigqueryQuote::quoteSingleIdentifier($projectId),
                BigqueryQuote::quote($wsResponse1->getWorkspaceObjectName()),
            )),
        );

        $this->assertNull($datasets->getIterator()->current());

        $cloudResourceManager = $this->clientManager->getCloudResourceManager($this->projectCredentials);
        $actualPolicy = $cloudResourceManager->projects->getIamPolicy(
            'projects/' . $projectId,
            (new GetIamPolicyRequest()),
            [],
        );
        $actualPolicy = $actualPolicy->getBindings();

        $serviceAccRoles = [];
        foreach ($actualPolicy as $policy) {
            $membersString = json_encode($policy->getMembers());
            assert(is_string($membersString));
            if (stripos($membersString, 'deleted:serviceAccount:' . $wsServiceAccEmail) !== false) {
                $serviceAccRoles[] = $policy->getRole();
            }
        }

        $this->assertEmpty($serviceAccRoles);
    }

    public function testDropWorkspaceWhenDatasetIsDeleted(): void
    {
        // CREATE
        [
            $credentials,
            $response,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse, $this->projects[0][2]);
        $this->assertInstanceOf(GenericBackendCredentials::class, $credentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $wsKeyData = CredentialsHelper::getCredentialsArray($credentials);

        Helper::assertServiceAccountBindings(
            $this->clientManager->getCloudResourceManager($this->projectCredentials),
            $wsKeyData['project_id'],
            $wsKeyData['client_email'],
            $this->log,
        );

        // drop workspace dataset and call drop workspace which must pass
        $bqClient->dataset($response->getWorkspaceObjectName())->delete(['deleteContents' => true]);

        // DROP
        $handler = new DropWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName())
            ->setWorkspaceRoleName($response->getWorkspaceRoleName())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());

        $dropResponse = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertNull($dropResponse);
    }

    public function testCreateDropCascadeWorkspace(): void
    {
        // CREATE
        [
            $credentials,
            $response,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse, $this->projects[0][2]);
        $this->assertInstanceOf(GenericBackendCredentials::class, $credentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $credentials);

        // create table
        $wsBqClient->executeQuery($wsBqClient->query(sprintf(
            'CREATE TABLE %s.`testTable` (`id` INTEGER);',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName()),
        )));

        $projectBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // try to DROP - should fail, there is a table
        $handler = new DropWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName())
            ->setWorkspaceRoleName($response->getWorkspaceRoleName())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());
        try {
            $handler(
                $this->projectCredentials,
                $command,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('Should fail as workspace database contains table');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                'is still in use',
                $e->getMessage(),
            );
        }

        $wsKeyData = CredentialsHelper::getCredentialsArray($credentials);
        $projectId = $wsKeyData['project_id'];
        $wsServiceAccEmail = $wsKeyData['client_email'];

        $datasets = $projectBqClient->executeQuery($projectBqClient->query(sprintf(
            /** @lang BigQuery */
            'SELECT `schema_name` FROM %s.INFORMATION_SCHEMA.SCHEMATA WHERE `schema_name` = %s ;',
            BigqueryQuote::quoteSingleIdentifier($projectId),
            BigqueryQuote::quote(strtoupper($response->getWorkspaceObjectName())),
        )));

        // ws dataset exist
        $this->assertCount(1, $datasets);

        // check if ws service acc still exist
        $iamService = $this->clientManager->getIamClient($this->projectCredentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $serviceAccountUrl = sprintf('projects/%s/serviceAccounts/%s', $projectId, $wsServiceAccEmail);
        $wsServiceAcc = $serviceAccountsService->get($serviceAccountUrl);
        $this->assertNotNull($wsServiceAcc);

        // try to DROP - should not fail and database will be deleted
        $command->setIsCascade(true);
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        (new RetryProxy(new SimpleRetryPolicy(5), new FixedBackOffPolicy()))
            ->call(function () use ($serviceAccountsService, $serviceAccountUrl): void {
                try {
                    $serviceAccountsService->get($serviceAccountUrl);
                    $this->fail(sprintf('Service account "%s" should be deleted.', $serviceAccountUrl));
                } catch (Throwable $e) {
                    $this->assertEquals(404, $e->getCode());
                    $this->assertStringContainsString('.iam.gserviceaccount.com does not exist.', $e->getMessage());
                }
            });

        $datasets = $projectBqClient->executeQuery($projectBqClient->query(sprintf(
        /** @lang BigQuery */
            'SELECT `schema_name` FROM %s.INFORMATION_SCHEMA.SCHEMATA WHERE `schema_name` = %s ;',
            BigqueryQuote::quoteSingleIdentifier($projectId),
            BigqueryQuote::quote(strtoupper($response->getWorkspaceObjectName())),
        )));

        $this->assertNull($datasets->getIterator()->current());
    }

    private function createNonEmptyTableInBucket(string $bucketDatasetName, string $tableName): void
    {
        // CREATE TABLE
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
            'primaryKeysNames' => [''],
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

        $handler = new ImportTableFromFileHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }
}
