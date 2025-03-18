<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\AddPrimaryKeyHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\CannotAddPrimaryKeyException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\DropPrimaryKeyHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\AddPrimaryKeyCommand;
use Keboola\StorageDriver\Command\Table\DropPrimaryKeyCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

class PrimaryKeyTest extends BaseCase
{
    protected CreateBucketResponse $bucketResponse;

    private GenericBackendCredentials $projectCredentials;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];

        $this->bucketResponse = $this->createTestBucket($this->projects[0][0]);
    }

    public function testAddDropPK(): void
    {
        $tableName = $this->getTestHash() . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'col1' => [
                    'type' => BigQuery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'col2' => [
                    'type' => BigQuery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'col3' => [
                    'type' => BigQuery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
            ],
            'primaryKeysNames' => [],
        ];
        $this->createTable($this->projectCredentials, $bucketDatabaseName, $tableName, $tableStructure);
        $this->fillTableWithData(
            $this->projectCredentials,
            $bucketDatabaseName,
            $tableName,
            [['columns' => '`col1`,`col2`,`col3`', 'rows' => ['1,2,3', '4,5,6']]],
        );

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $pkNames = new RepeatedField(GPBType::STRING);
        $pkNames[] = 'col2';
        $pkNames[] = 'col3';

        // add PK
        $addPKCommand = (new AddPrimaryKeyCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setPrimaryKeysNames($pkNames);
        $addPKHandler = new AddPrimaryKeyHandler($this->clientManager);
        $addPKHandler->setInternalLogger($this->log);
        $addPKHandler(
            $this->projectCredentials,
            $addPKCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $bqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->projectCredentials,
        );
        // check the existence of PK via table reflection
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $tableName);
        $this->assertEquals(['col2', 'col3'], $ref->getPrimaryKeysNames());

        // drop PK
        $dropPKCommand = (new DropPrimaryKeyCommand())
            ->setPath($path)
            ->setTableName($tableName);
        $dropPKHandler = new DropPrimaryKeyHandler($this->clientManager);
        $addPKHandler->setInternalLogger($this->log);
        $dropPKHandler(
            $this->projectCredentials,
            $dropPKCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // check the non-existence of PK via table reflection
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $tableName);
        $this->assertEquals([], $ref->getPrimaryKeysNames());
    }

    public function testDuplicates(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'col1' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'col2' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'col3' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
            ],
            'primaryKeysNames' => [],
        ];
        $this->createTable($this->projectCredentials, $bucketDatabaseName, $tableName, $tableStructure);

        $this->fillTableWithData(
            $this->projectCredentials,
            $bucketDatabaseName,
            $tableName,
            [['columns' => '`col1`,`col2`,`col3`', 'rows' => ['1,5,6', '1,5,6']]],
        );

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $pkNames = new RepeatedField(GPBType::STRING);
        $pkNames[] = 'col2';
        $pkNames[] = 'col3';

        // add PK
        $addPKCommand = (new AddPrimaryKeyCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setPrimaryKeysNames($pkNames);
        $addPKHandler = new AddPrimaryKeyHandler($this->clientManager);
        $addPKHandler->setInternalLogger($this->log);

        $this->expectException(CannotAddPrimaryKeyException::class);
        $this->expectExceptionMessage('Selected columns contain duplicities');
        $addPKHandler(
            $this->projectCredentials,
            $addPKCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    public function testPKExists(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'col1' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'col2' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'col3' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
            ],
            'primaryKeysNames' => ['col2'],
        ];
        $this->createTable($this->projectCredentials, $bucketDatabaseName, $tableName, $tableStructure);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $pkNames = new RepeatedField(GPBType::STRING);
        $pkNames[] = 'col2';
        $pkNames[] = 'col3';

        // add PK
        $addPKCommand = (new AddPrimaryKeyCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setPrimaryKeysNames($pkNames);
        $addPKHandler = new AddPrimaryKeyHandler($this->clientManager);
        $addPKHandler->setInternalLogger($this->log);

        $this->expectException(CannotAddPrimaryKeyException::class);
        $this->expectExceptionMessage('Primary key already exists');
        $addPKHandler(
            $this->projectCredentials,
            $addPKCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    public function testColumnIsNullable(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'col1' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'col2' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => true,
                ],
                'col3' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
            ],
            'primaryKeysNames' => [],
        ];
        $this->createTable($this->projectCredentials, $bucketDatabaseName, $tableName, $tableStructure);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $pkNames = new RepeatedField(GPBType::STRING);
        $pkNames[] = 'col2';
        $pkNames[] = 'col3';

        // add PK
        $addPKCommand = (new AddPrimaryKeyCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setPrimaryKeysNames($pkNames);
        $addPKHandler = new AddPrimaryKeyHandler($this->clientManager);
        $addPKHandler->setInternalLogger($this->log);

        $this->expectException(CannotAddPrimaryKeyException::class);
        $this->expectExceptionMessage('Selected column "col2" is nullable');
        $addPKHandler(
            $this->projectCredentials,
            $addPKCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }
}
