<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

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
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

class CreateDropWorkspaceTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$credentials, $response] = $this->createTestProject();
        $this->projectCredentials = $credentials;
        $this->projectResponse = $response;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testCreateDropWorkspace(): void
    {
        // CREATE
        [$credentials, $response] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);
        $this->assertInstanceOf(GenericBackendCredentials::class, $credentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $wsKeyData = CredentialsHelper::getCredentialsArray($credentials);
        $projectId = $wsKeyData['project_id'];
        $wsServiceAccEmail = $wsKeyData['client_email'];

        // check objects created
        $iamService = $this->clientManager->getIamClient($this->projectCredentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $wsServiceAcc = $serviceAccountsService->get(
            sprintf('projects/%s/serviceAccounts/%s', $projectId, $wsServiceAccEmail)
        );
        $this->assertNotNull($wsServiceAcc);

        $wsBqClient = $this->clientManager->getBigQueryClient($credentials);

        /** @var array<string, string> $datasets */
        $datasets = $bqClient->runQuery($bqClient->query(sprintf('SELECT
  schema_name
FROM
  %s.INFORMATION_SCHEMA.SCHEMATA;', BigqueryQuote::quoteSingleIdentifier($projectId))))->getIterator()->current();

        $this->assertSame(
            strtoupper($response->getWorkspaceObjectName()),
            $datasets['schema_name']
        );

        // test ws service acc is owner of ws dataset
        $workspaceDataset = $bqClient->dataset($response->getWorkspaceObjectName())->info();
        $this->assertNotNull($workspaceDataset);
        $this->assertCount(1, $workspaceDataset['access']);
        $this->assertSame('OWNER', $workspaceDataset['access'][0]['role']);
        $this->assertSame($wsServiceAccEmail, $workspaceDataset['access'][0]['userByEmail']);

        $cloudResourceManager = $this->clientManager->getCloudResourceManager($this->projectCredentials);
        $actualPolicy = $cloudResourceManager->projects->getIamPolicy(
            'projects/' . $projectId,
            (new GetIamPolicyRequest()),
            []
        );
        $actualPolicy = $actualPolicy->getBindings();

        $serviceAccRoles = [];
        foreach ($actualPolicy as $policy) {
            if (in_array('serviceAccount:' . $wsServiceAccEmail, $policy->getMembers())) {
                $serviceAccRoles[] = $policy->getRole();
            }
        }

        // ws service acc must have a job user role to be able to run queries
        $expected = [
            IAmPermissions::ROLES_BIGQUERY_DATA_VIEWER, // readOnly access
            IAmPermissions::ROLES_BIGQUERY_JOB_USER,
        ];
        $this->assertEqualsArrays($expected, $serviceAccRoles);

        try {
            $this->createBucketInProject($credentials, 'should_fail');
            $this->fail('The workspace user should not have the right to create a new dataset.');
        } catch (ServiceException $exception) {
            $this->assertSame(403, $exception->getCode());
            $this->assertStringContainsString(
                'User does not have bigquery.datasets.create permission in project',
                $exception->getMessage()
            );
        }

        $bucketName = 'testReadOnlySchema';
        $tableName = 'testTable';

        $bucketDatasetName = $this->createBucketInProject($this->projectCredentials, $bucketName);
        $this->createNonEmptyTableInBucket($bucketDatasetName, $tableName);

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
            $this->fillTableWithData($credentials, $bucketDatasetName, $tableName, $insertGroups);
            $this->fail('Insert to read only table should failed');
        } catch (ServiceException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertStringContainsString('Access Denied: ', $e->getMessage());
        }

        $result = $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'SELECT * FROM %s.%s;',
            BigqueryQuote::quoteSingleIdentifier($bucketName),
            BigqueryQuote::quoteSingleIdentifier($tableName)
        )));

        $this->assertCount(3, $result);
        // try to create table
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'CREATE TABLE %s.`testTable` (`id` INTEGER);',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        // try to create view
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'CREATE VIEW %s.`testView` AS '
            . 'SELECT `id` FROM %s.`testTable`;',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName()),
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        // try to drop view
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'DROP VIEW %s.`testView`;',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        // try to drop table
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'DROP TABLE %s.`testTable`;',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        // DROP
        $handler = new DropWorkspaceHandler($this->clientManager);
        $command = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName())
            ->setWorkspaceRoleName($response->getWorkspaceRoleName())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());

        $dropResponse = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(),
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

        $datasets = $bqClient->runQuery(
            $bqClient->query(sprintf(
                'SELECT schema_name FROM %s.INFORMATION_SCHEMA.SCHEMATA WHERE `schema_name` = %s;',
                BigqueryQuote::quoteSingleIdentifier($projectId),
                BigqueryQuote::quote($response->getWorkspaceObjectName())
            ))
        );

        $this->assertNull($datasets->getIterator()->current());

        $cloudResourceManager = $this->clientManager->getCloudResourceManager($this->projectCredentials);
        $actualPolicy = $cloudResourceManager->projects->getIamPolicy(
            'projects/' . $projectId,
            (new GetIamPolicyRequest()),
            []
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

    public function testCreateDropCascadeWorkspace(): void
    {
        // CREATE
        [$credentials, $response] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);
        $this->assertInstanceOf(GenericBackendCredentials::class, $credentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $wsBqClient = $this->clientManager->getBigQueryClient($credentials);

        // create table
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'CREATE TABLE %s.`testTable` (`id` INTEGER);',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        $projectBqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);

        // try to DROP - should fail, there is a table
        $handler = new DropWorkspaceHandler($this->clientManager);
        $command = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName())
            ->setWorkspaceRoleName($response->getWorkspaceRoleName())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());
        try {
            $handler(
                $this->projectCredentials,
                $command,
                [],
                new RuntimeOptions(),
            );
            $this->fail('Should fail as workspace database contains table');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                'is still in use',
                $e->getMessage()
            );
        }

        $wsKeyData = CredentialsHelper::getCredentialsArray($credentials);
        $projectId = $wsKeyData['project_id'];
        $wsServiceAccEmail = $wsKeyData['client_email'];

        /** @var array<string, string> $datasets */
        $datasets = $projectBqClient->runQuery($projectBqClient->query(sprintf('SELECT
  schema_name
FROM
  %s.INFORMATION_SCHEMA.SCHEMATA;', BigqueryQuote::quoteSingleIdentifier($projectId))))->getIterator()->current();

        // ws dataset exist
        $this->assertSame(
            strtoupper($response->getWorkspaceObjectName()),
            $datasets['schema_name']
        );

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
            new RuntimeOptions(),
        );

        try {
            $serviceAccountsService->get($serviceAccountUrl);
            $this->fail(sprintf('Service account "%s" should be deleted.', $serviceAccountUrl));
        } catch (GoogleServiceException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertStringContainsString('.iam.gserviceaccount.com does not exist.', $e->getMessage());
        }

        $datasets = $projectBqClient->runQuery($projectBqClient->query(sprintf('SELECT
  schema_name
FROM
  %s.INFORMATION_SCHEMA.SCHEMATA;', BigqueryQuote::quoteSingleIdentifier($projectId))));

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
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::NONE)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath('import')
                ->setFileName('a_b_c-3row.csv')
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($tableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->clientManager);
        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(),
        );
    }

    private function createBucketInProject(GenericBackendCredentials $credentials, string $bucketName): string
    {
        $handler = new CreateBucketHandler($this->clientManager);
        $command = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($this->getProjectId())
            ->setBucketId($bucketName);

        /** @var CreateBucketResponse $bucketResponse */
        $bucketResponse = $handler(
            $credentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        return $bucketResponse->getCreateBucketObjectName();
    }
}
