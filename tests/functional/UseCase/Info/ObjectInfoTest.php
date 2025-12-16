<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Info;

use Google\Cloud\BigQuery\Connection\V1\Client\ConnectionServiceClient;
use Google\Cloud\BigQuery\Connection\V1\CloudResourceProperties;
use Google\Cloud\BigQuery\Connection\V1\Connection;
use Google\Cloud\BigQuery\Connection\V1\CreateConnectionRequest;
use Google\Cloud\Core\Exception\ServiceException;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\Handler\Info\ObjectInfoHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\LogMessage;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Info\ObjectInfo;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\SchemaInfo;
use Keboola\StorageDriver\Command\Info\TableInfo\TableColumn;
use Keboola\StorageDriver\Command\Info\TableType;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;
use Traversable;

class ObjectInfoTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    private CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];
        $this->projectResponse = $this->projects[0][1];

        // create bucket
        $this->bucketResponse = $this->createTestBucket($this->projects[0][0]);

        $this->createTestTable(
            $this->projectCredentials,
            $this->bucketResponse->getCreateBucketObjectName(),
            $this->getTestHash(),
        );
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE VIEW %s.`bucket_view1` AS '
            . 'SELECT * FROM %s.%s;',
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($this->getTestHash()),
        )));
    }

    public function testInfoDatabase(): void
    {
        // create workspace
        [
            ,
            $workspaceResponse,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projects[0][1]);
        $this->createTestTable(
            $this->projectCredentials,
            $workspaceResponse->getWorkspaceObjectName(),
            'ws_table1',
        );

        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        // expect database
        $command->setExpectedObjectType(ObjectType::DATABASE);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([$this->projectResponse->getProjectUserName()]));
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::DATABASE, $response->getObjectType());
        $this->assertTrue($response->hasDatabaseInfo());
        $this->assertFalse($response->hasSchemaInfo());
        $this->assertFalse($response->hasTableInfo());
        $this->assertFalse($response->hasViewInfo());
        $this->assertNotNull($response->getDatabaseInfo());
        $this->assertSame(
            [$this->projectResponse->getProjectUserName()],
            ProtobufHelper::repeatedStringToArray($response->getPath()),
        );
        /** @var Traversable<ObjectInfo> $objects */
        $objects = $response->getDatabaseInfo()->getObjects()->getIterator();
        $bucketObject = $this->getObjectByNameAndType(
            $objects,
            $this->bucketResponse->getCreateBucketObjectName(),
        );
        $this->assertSame(ObjectType::SCHEMA, $bucketObject->getObjectType());
        $workspaceObject = $this->getObjectByNameAndType(
            $objects,
            $workspaceResponse->getWorkspaceObjectName(),
        );
        $this->assertSame(ObjectType::SCHEMA, $workspaceObject->getObjectType());
    }

    public function testInfoSchema(): void
    {
        $this->createObjectsInSchema();
        // Run object info cmd
        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        $command->setExpectedObjectType(ObjectType::SCHEMA);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([$this->bucketResponse->getCreateBucketObjectName()]));
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::SCHEMA, $response->getObjectType());
        $this->assertSame(
            [$this->bucketResponse->getCreateBucketObjectName()],
            ProtobufHelper::repeatedStringToArray($response->getPath()),
        );
        $this->assertFalse($response->hasDatabaseInfo());
        $this->assertTrue($response->hasSchemaInfo());
        $this->assertFalse($response->hasTableInfo());
        $this->assertFalse($response->hasViewInfo());
        $this->assertNotNull($response->getSchemaInfo());
        /** @var Traversable<ObjectInfo> $objects */
        $objects = $response->getSchemaInfo()->getObjects()->getIterator();

        // can select from both external tables, because main project has access to files in GCS
        $this->assertCount(9, $objects);
        $table = $this->getObjectByNameAndType(
            $objects,
            $this->getTestHash(),
        );
        $this->assertSame(ObjectType::TABLE, $table->getObjectType());
        $table = $this->getObjectByNameAndType(
            $objects,
            'snapshot',
        );
        $this->assertSame(ObjectType::TABLE, $table->getObjectType());
        $view = $this->getObjectByNameAndType(
            $objects,
            'bucket_view1',
        );
        $this->assertSame(ObjectType::VIEW, $view->getObjectType());
        // two external bucket are available, in second project we test that external tables are not supported
        // if we haven't access to GCS
        $externalTable = $this->getObjectByNameAndType(
            $objects,
            'externalTable',
        );
        $this->assertSame(ObjectType::TABLE, $externalTable->getObjectType());
        $externalTableWithConnection = $this->getObjectByNameAndType(
            $objects,
            'externalTableWithConnection',
        );
        $this->assertSame(ObjectType::TABLE, $externalTableWithConnection->getObjectType());
        $materializedView = $this->getObjectByNameAndType(
            $objects,
            'materialized_view',
        );
        $this->assertSame(ObjectType::VIEW, $materializedView->getObjectType());
        $partitionedTable = $this->getObjectByNameAndType(
            $objects,
            'partitionedTable',
        );
        $this->assertSame(ObjectType::TABLE, $partitionedTable->getObjectType());
        $partitionedView = $this->getObjectByNameAndType(
            $objects,
            'partitionedView',
        );
        $this->assertSame(ObjectType::VIEW, $partitionedView->getObjectType());

        /** @var LogMessage[] $logs */
        $logs = iterator_to_array($handler->getMessages()->getIterator());
        $this->assertCount(6, $logs);
        $this->assertLogsContainsMessage(
            $logs,
            LogMessage\Level::Informational,
            sprintf(
                'The view "%s:%s.partitionedView" has a partition filter set, which stops us from verifying if it can be read.', //phpcs:ignore
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
            ),
        );
        $this->assertLogsContainsMessage(
            $logs,
            LogMessage\Level::Warning,
            sprintf(
                'Selecting data from view "%s:%s.table2View" failed with error: ' .
                '"Not found: Table %s:%s.table2 was not found in location US" View was ignored',
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
            ),
        );

        $this->assertLogsContainsMessage(
            $logs,
            LogMessage\Level::Warning,
            sprintf(
                'We have registered an external table: "%s:%s.externalTable". Please note, if this table is not created as a BigLake table, reading from it in the workspace will not be possible.', //phpcs:ignore
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
            ),
        );

        $this->assertLogsContainsMessage(
            $logs,
            LogMessage\Level::Warning,
            sprintf(
                'We have registered an external table: "%s:%s.externalTableWithConnection". Please note, if this table is not created as a BigLake table, reading from it in the workspace will not be possible.', //phpcs:ignore
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
            ),
        );

        // Create workspace
        [
            $credentials,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);

        // Run same object info cmd but as workspace user
        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        $command->setExpectedObjectType(ObjectType::SCHEMA);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([$this->bucketResponse->getCreateBucketObjectName()]));
        $response = $handler(
            $credentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertInstanceOf(SchemaInfo::class, $response->getSchemaInfo());
        /** @var Traversable<ObjectInfo> $objects */
        $objects = $response->getSchemaInfo()->getObjects()->getIterator();
        // in link project we can select only from external tables with connection
        // second external table without connection is ignored, and warning is logged
        $this->assertCount(8, $objects);
        $table = $this->getObjectByNameAndType(
            $objects,
            $this->getTestHash(),
        );
        $this->assertSame(ObjectType::TABLE, $table->getObjectType());
        $table = $this->getObjectByNameAndType(
            $objects,
            'snapshot',
        );
        $this->assertSame(ObjectType::TABLE, $table->getObjectType());
        $view = $this->getObjectByNameAndType(
            $objects,
            'bucket_view1',
        );
        $this->assertSame(ObjectType::VIEW, $view->getObjectType());
        // in link project, we only can access to external table with connection
        $externalTableWithConnection = $this->getObjectByNameAndType(
            $objects,
            'externalTableWithConnection',
        );
        $this->assertSame(ObjectType::TABLE, $externalTableWithConnection->getObjectType());
        $view = $this->getObjectByNameAndType(
            $objects,
            'materialized_view',
        );
        $this->assertSame(ObjectType::VIEW, $view->getObjectType());
        $view = $this->getObjectByNameAndType(
            $objects,
            'partitionedTable',
        );
        $this->assertSame(ObjectType::TABLE, $view->getObjectType());
        $view = $this->getObjectByNameAndType(
            $objects,
            'partitionedView',
        );
        $this->assertSame(ObjectType::VIEW, $view->getObjectType());

        /** @var LogMessage[] $logs */
        $logs = iterator_to_array($handler->getMessages()->getIterator());
        $this->assertCount(6, $logs);
        $this->assertLogsContainsMessage(
            $logs,
            LogMessage\Level::Informational,
            sprintf(
                'The view "%s:%s.partitionedView" has a partition filter set, which stops us from verifying if it can be read.', //phpcs:ignore
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
            ),
        );
        $this->assertLogsContainsMessage(
            $logs,
            LogMessage\Level::Warning,
            sprintf(
                'Selecting data from view "%s:%s.table2View" failed with error: ' .
                '"Not found: Table %s:%s.table2 was not found in location US" View was ignored',
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
            ),
        );
        $this->assertLogsContainsMessage(
            $logs,
            LogMessage\Level::Warning,
            sprintf(
                'Unable to read from the external table. The table named "%s:%s.%s" has been skipped.',
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
                'externalTable',
            ),
        );
        $this->assertLogsContainsMessage(
            $logs,
            LogMessage\Level::Warning,
            sprintf(
                'We have registered an external table: "%s:%s.externalTableWithConnection". Please note, if this table is not created as a BigLake table, reading from it in the workspace will not be possible.', //phpcs:ignore
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
            ),
        );
    }

    private function createObjectsInSchema(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE MATERIALIZED VIEW %s.`materialized_view` AS '
            . 'SELECT * FROM %s.%s;',
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($this->getTestHash()),
        )));
        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE SNAPSHOT TABLE %s.`snapshot` CLONE %s.%s '
            . 'OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR));',
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($this->getTestHash()),
        )));
        $bqClient->runQuery($bqClient->query(sprintf(
            "CREATE OR REPLACE EXTERNAL TABLE %s.externalTable OPTIONS (format = 'CSV',uris = [%s]);",
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quote('gs://' . getenv('BQ_BUCKET_NAME') . '/import/a_b_c-3row.csv'),
        )));

        // simulate user interaction, he creates connection to external bucket manually in console.google.com
        $connection = $this->prepareConnectionForExternalBucket();

        $bqClient->runQuery($bqClient->query(sprintf(
            "CREATE OR REPLACE EXTERNAL TABLE %s.externalTableWithConnection 
            WITH CONNECTION %s 
            OPTIONS (format = 'CSV',uris = [%s]);",
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($connection->getName()),
            BigqueryQuote::quote('gs://' . getenv('BQ_BUCKET_NAME') . '/import/a_b_c-3row.csv'),
        )));

        $bqClient->runQuery($bqClient->query(sprintf(
        /** @lang BigQuery */<<<SQL
CREATE TABLE
  %s.partitionedTable (transaction_id INT64)
PARTITION BY
  _PARTITIONDATE
  OPTIONS (
    PARTITION_EXPIRATION_DAYS = 3,
    REQUIRE_PARTITION_FILTER = TRUE);
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
        )));

        $bqClient->runQuery($bqClient->query(sprintf(
        /** @lang BigQuery */<<<SQL
INSERT INTO %s.partitionedTable (transaction_id) VALUES (1)
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
        )));

        $bqClient->runQuery($bqClient->query(sprintf(
        /** @lang BigQuery */<<<SQL
CREATE VIEW %s.partitionedView AS ( 
    SELECT
      *
    FROM
      %s.partitionedTable
)
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
        )));

        // partitioned table using hive
        $bqClient->runQuery(
            $bqClient->query(
                sprintf(
                    <<<SQL
CREATE OR REPLACE EXTERNAL TABLE %s.externalTableWithConnectionAndPartitioning 
            WITH PARTITION COLUMNS (
              part INT64,
            )
            WITH CONNECTION %s 
            OPTIONS (
            format = 'CSV',
            uris = [%s],
            max_staleness=INTERVAL '0-0 0 0:30:0' YEAR TO SECOND,
            metadata_cache_mode = 'AUTOMATIC',
            hive_partition_uri_prefix=%s,
            require_hive_partition_filter=true
            );
SQL,
                    BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
                    BigqueryQuote::quoteSingleIdentifier($connection->getName()),
                    implode(
                        ',',
                        [
                            BigqueryQuote::quote('gs://' . getenv('BQ_BUCKET_NAME') . '/hive/part=1/*'),
                            BigqueryQuote::quote('gs://' . getenv('BQ_BUCKET_NAME') . '/hive/part=2/*'),
                        ],
                    ),
                    BigqueryQuote::quote('gs://' . getenv('BQ_BUCKET_NAME') . '/hive/'),
                ),
            ),
        );

        // partitioned table but with wrong partitioning set
        $bqClient->runQuery(
            $bqClient->query(
                sprintf(
                    <<<SQL
CREATE OR REPLACE EXTERNAL TABLE %s.externalTableWithInvalidPartitioning 
            WITH PARTITION COLUMNS (
              part INT64,
            )
            WITH CONNECTION %s 
            OPTIONS (
            format = 'CSV',
            uris = [%s],
            max_staleness=INTERVAL '0-0 0 0:30:0' YEAR TO SECOND,
            metadata_cache_mode = 'AUTOMATIC',
            hive_partition_uri_prefix=%s
            );
SQL,
                    BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
                    BigqueryQuote::quoteSingleIdentifier($connection->getName()),
                    implode(
                        ',',
                        [
                            BigqueryQuote::quote('gs://' . getenv('BQ_BUCKET_NAME') . '/test_users_with_role.csv'),
                        ],
                    ),
                    BigqueryQuote::quote('gs://' . getenv('BQ_BUCKET_NAME')),
                ),
            ),
        );

        $bqClient->runQuery($bqClient->query(sprintf(
        /** @lang BigQuery */<<<SQL
CREATE TABLE
  %s.table2 (transaction_id INT64)
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
        )));

        $bqClient->runQuery($bqClient->query(sprintf(
        /** @lang BigQuery */<<<SQL
INSERT INTO %s.table2 (transaction_id) VALUES (1)
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
        )));

        $bqClient->runQuery($bqClient->query(sprintf(
        /** @lang BigQuery */<<<SQL
CREATE VIEW %s.table2View AS ( 
    SELECT
      *
    FROM
      %s.table2
)
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
        )));
        $bqClient->runQuery($bqClient->query(sprintf(
        /** @lang BigQuery */<<<SQL
DROP TABLE %s.table2
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
        )));
    }

    /**
     * @throws \Google\ApiCore\ApiException
     * @throws \Google\ApiCore\ValidationException
     * @throws \JsonException
     */
    private function prepareConnectionForExternalBucket(): Connection
    {
        $externalCredentialsArray = CredentialsHelper::getCredentialsArray($this->projectCredentials);
        $connectionClient = new ConnectionServiceClient([
            'credentials' => $externalCredentialsArray,
        ]);

        // 1. create connection to be able to read from external bucket
        $parent = $connectionClient->locationName($externalCredentialsArray['project_id'], 'US');
        $cloudSqlProperties = (new CloudResourceProperties());
        $connection = (new Connection())
            ->setFriendlyName('externalTableConnection')
            ->setCloudResource($cloudSqlProperties);

        $request = (new CreateConnectionRequest())
            ->setParent($parent)
            ->setConnectionId('exConn-' . substr($this->getTestHash(), -7) . self::getRand())
            ->setConnection($connection);

        $response = $connectionClient->createConnection($request);
        // when connection is created, new service account is created for connection automatically
        $connectionServiceAccountEmail = $response->getCloudResource()?->getServiceAccountId();

        // 2. connection was created, now we need to grant access to service account created for connection
        $storageClient = $this->clientManager->getStorageClient($this->getCredentials());
        $bqBucketName = getenv('BQ_BUCKET_NAME');
        assert($bqBucketName !== false, 'BQ_BUCKET_NAME env var is not set');
        $bucket = $storageClient->bucket($bqBucketName);

        // Retry logic for IAM policy updates to handle concurrent modification (412 errors)
        // When multiple tests run in parallel, they may try to update the same bucket's IAM policy
        // If the policy changes between read and write, setPolicy() fails with 412 (ETag mismatch)
        $role = 'roles/storage.objectViewer';
        $retryPolicy = new CallableRetryPolicy(function (Throwable $e): bool {
            // Only retry on 412 (ETag mismatch / concurrent modification)
            return $e instanceof ServiceException && $e->getCode() === 412;
        }, 5);
        $backOffPolicy = new ExponentialRandomBackOffPolicy(
            1_000, // 1s
            1.8,
            10_000, // 10s
        );
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $proxy->call(function () use ($bucket, $role, $connectionServiceAccountEmail): void {
            $policy = $bucket->iam()->policy();
            $policy['bindings'][] = [
                'role' => $role,
                'members' => [
                    'serviceAccount:' . $connectionServiceAccountEmail,
                ],
            ];
            $bucket->iam()->setPolicy($policy);
        });
        return $response;
    }

    public function testInfoTable(): void
    {
        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        $command->setExpectedObjectType(ObjectType::TABLE);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([
            $this->bucketResponse->getCreateBucketObjectName(),
            $this->getTestHash(),
        ]));
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertSame(
            [
                $this->bucketResponse->getCreateBucketObjectName(),
                $this->getTestHash(),
            ],
            ProtobufHelper::repeatedStringToArray($response->getPath()),
        );
        $this->assertFalse($response->hasDatabaseInfo());
        $this->assertFalse($response->hasSchemaInfo());
        $this->assertTrue($response->hasTableInfo());
        $this->assertFalse($response->hasViewInfo());

        $tableInfo = $response->getTableInfo();
        $this->assertNotNull($tableInfo);
        $this->assertSame($this->getTestHash(), $tableInfo->getTableName());
        $this->assertSame(
            [$this->bucketResponse->getCreateBucketObjectName()],
            ProtobufHelper::repeatedStringToArray($tableInfo->getPath()),
        );
        $this->assertSame(
            ['id'],
            ProtobufHelper::repeatedStringToArray($tableInfo->getPrimaryKeysNames()),
        );
        /** @var TableColumn[] $columns */
        $columns = iterator_to_array($tableInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableColumn $col) => $col->getName(),
            $columns,
        );
        $this->assertSame(['id', 'name', 'large'], $columnsNames);
        $this->assertEquals(TableType::NORMAL, $tableInfo->getTableType());
    }

    public function testExternalTableType(): void
    {
        $this->createObjectsInSchema();

        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        $command->setExpectedObjectType(ObjectType::TABLE);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([
            $this->bucketResponse->getCreateBucketObjectName(),
            'externalTable',
        ]));

        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $tableInfo = $response->getTableInfo();
        $this->assertNotNull($tableInfo);

        $this->assertEquals(TableType::EXTERNAL, $tableInfo->getTableType());
    }

    public function testExternalPartitionedTableType(): void
    {
        $this->createObjectsInSchema();

        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        $command->setExpectedObjectType(ObjectType::SCHEMA);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([
            $this->bucketResponse->getCreateBucketObjectName(),
        ]));

        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        /** @var Traversable<ObjectInfo> $objects */
        $objects = $response->getSchemaInfo()?->getObjects()->getIterator();

        $table = $this->getObjectByNameAndType(
            $objects,
            'externalTableWithConnectionAndPartitioning',
        );
        $this->assertSame(ObjectType::TABLE, $table->getObjectType());

        /** @var LogMessage[] $logs */
        $logs = iterator_to_array($handler->getMessages()->getIterator());
        $this->assertLogsContainsMessage(
            $logs,
            LogMessage\Level::Warning,
            sprintf(
                'Cannot query over table \'%s.%s.%s\' without a filter over column(s) \'part\' that can be used for partition elimination', //phpcs:ignore
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
                'externalTableWithConnectionAndPartitioning',
            ),
        );
    }

    public function testExternalPartitionedTableFail(): void
    {
        $this->createObjectsInSchema();

        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        $command->setExpectedObjectType(ObjectType::SCHEMA);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([
            $this->bucketResponse->getCreateBucketObjectName(),
        ]));

        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(ObjectInfoResponse::class, $response);

        /** @var LogMessage[] $logs */
        $logs = iterator_to_array($handler->getMessages()->getIterator());
        $this->assertLogsContainsMessage(
            $logs,
            LogMessage\Level::Warning,
            sprintf(
                'Unable to read from the external table. The table named "%s:%s.%s" has been skipped. Original error from BigQuery: "Incompatible partition schemas.  Expected schema ([part:TYPE_INT64]) has 1 columns. Observed schema ([]) has 0 columns.".', //phpcs:ignore
                CredentialsHelper::getCredentialsArray($this->projectCredentials)['project_id'],
                $this->bucketResponse->getCreateBucketObjectName(),
                'externalTableWithInvalidPartitioning',
            ),
        );
    }

    public function testInfoView(): void
    {
        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        $command->setExpectedObjectType(ObjectType::VIEW);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([
            $this->bucketResponse->getCreateBucketObjectName(),
            'bucket_view1',
        ]));
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::VIEW, $response->getObjectType());
        $this->assertSame(
            [
                $this->bucketResponse->getCreateBucketObjectName(),
                'bucket_view1',
            ],
            ProtobufHelper::repeatedStringToArray($response->getPath()),
        );
        $this->assertFalse($response->hasDatabaseInfo());
        $this->assertFalse($response->hasSchemaInfo());
        $this->assertFalse($response->hasTableInfo());
        $this->assertTrue($response->hasViewInfo());

        $viewInfo = $response->getViewInfo();

        $this->assertNotNull($viewInfo);
        $this->assertSame('bucket_view1', $viewInfo->getViewName());
        $this->assertSame(
            [$this->bucketResponse->getCreateBucketObjectName()],
            ProtobufHelper::repeatedStringToArray($viewInfo->getPath()),
        );
        /** @var TableColumn[] $columns */
        $columns = iterator_to_array($viewInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableColumn $col) => $col->getName(),
            $columns,
        );
        $this->assertSame(['id', 'name', 'large'], $columnsNames);
    }

    /** @param Traversable<ObjectInfo> $objects */
    private function getObjectByNameAndType(Traversable $objects, string $expectedName): ObjectInfo
    {
        foreach ($objects as $objectInfo) {
            if ($objectInfo->getObjectName() === $expectedName) {
                return $objectInfo;
            }
        }
        $this->fail(sprintf('Expected object name "%s" not found.', $expectedName));
    }

    /**
     * @param LogMessage[] $logs
     */
    private function assertLogsContainsMessage(array $logs, int $level, string $message): void
    {
        foreach ($logs as $log) {
            if ($log->getLevel() === $level && str_contains($log->getMessage(), $message)) {
                return;
            }
        }
        $this->fail(sprintf(
            'Expected log message "%s" not found. Messages were:%s %s',
            $message,
            PHP_EOL,
            implode(PHP_EOL, array_map(
                static fn(LogMessage $log) => sprintf(
                    '%s: %s',
                    LogMessage\Level::name($log->getLevel()),
                    $log->getMessage(),
                ),
                $logs,
            ),),
        ));
    }
}
