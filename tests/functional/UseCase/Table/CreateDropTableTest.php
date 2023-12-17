<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Generator;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\Backend\BigQuery\Clustering;
use Keboola\StorageDriver\Backend\BigQuery\RangePartitioning;
use Keboola\StorageDriver\Backend\BigQuery\TimePartitioning;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\BadTableDefinitionException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;

class CreateDropTableTest extends BaseCase
{
    protected CreateBucketResponse $bucketResponse;

    private GenericBackendCredentials $projectCredentials;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];

        $this->bucketResponse = $this->createTestBucket($this->projects[0][0], $this->projects[0][2]);
    }

    public function testCreateTable(): void
    {
        $tableName = $this->getTestHash() . '_Test_table';
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = (new TableColumnShared)
            ->setName('name')
            ->setType(Bigquery::TYPE_STRING)
            ->setLength('50')
            ->setNullable(true)
            ->setDefault("'Some Default'");
        $columns[] = (new TableColumnShared)
            ->setName('large')
            ->setType(Bigquery::TYPE_BIGNUMERIC)
            ->setLength('76,38')
            ->setDefault('185.554');
        $columns[] = (new TableColumnShared)
            ->setName('ordes')
            ->setType(Bigquery::TYPE_ARRAY)
            ->setLength('STRUCT<x ARRAY<STRUCT<xz ARRAY<INT64>>>>');
        $columns[] = (new TableColumnShared)
            ->setName('organization')
            ->setType(Bigquery::TYPE_STRUCT)
            ->setLength('x ARRAY<INT64>');
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns);
        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertNotNull($response->getTableInfo());

        $columns = $response->getTableInfo()->getColumns();
        $this->assertCount(5, $columns);

        // check column ID
        /** @var TableInfo\TableColumn $column */
        $column = $columns[0];
        $this->assertSame('id', $column->getName());
        $this->assertSame(Bigquery::TYPE_INTEGER, $column->getType());
        $this->assertFalse($column->getNullable());
        $this->assertSame('', $column->getDefault());

        // check column NAME
        /** @var TableInfo\TableColumn $column */
        $column = $columns[1];
        $this->assertSame('name', $column->getName());
        $this->assertSame(Bigquery::TYPE_STRING, $column->getType());
        $this->assertSame('50', $column->getLength());
        $this->assertTrue($column->getNullable());
        $this->assertSame("'Some Default'", $column->getDefault());

        // check column LARGE
        /** @var TableInfo\TableColumn $column */
        $column = $columns[2];
        $this->assertSame('large', $column->getName());
        $this->assertSame(Bigquery::TYPE_BIGNUMERIC, $column->getType());
        $this->assertSame('76,38', $column->getLength());
        $this->assertFalse($column->getNullable());
        $this->assertSame('185.554', $column->getDefault());

        // check column array
        /** @var TableInfo\TableColumn $column */
        $column = $columns[3];
        $this->assertSame('ordes', $column->getName());
        $this->assertSame(Bigquery::TYPE_ARRAY, $column->getType());
        $this->assertSame('STRUCT<x ARRAY<STRUCT<xz ARRAY<INTEGER>>>>', $column->getLength());
        $this->assertTrue($column->getNullable());

        // check column array
        /** @var TableInfo\TableColumn $column */
        $column = $columns[4];
        $this->assertSame('organization', $column->getName());
        $this->assertSame(Bigquery::TYPE_STRUCT, $column->getType());
        $this->assertSame('x ARRAY<INTEGER>', $column->getLength());
        $this->assertTrue($column->getNullable());

        // DROP TABLE
        $handler = new DropTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new DropTableCommand())
            ->setPath($path)
            ->setTableName($tableName);

        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        $bucket = $bqClient->dataset($bucketDatasetName);
        $this->assertTrue($bucket->exists());

        $table = $bucket->table($tableName);
        $this->assertFalse($table->exists());
    }

    public function testCreateTableWithPartitioningClustering(): void
    {
        // Range partitioning and clustering
        $tableInfo = $this->createTableForPartitioning(
            (new CreateTableCommand\BigQueryTableMeta())
                ->setClustering((new Clustering())->setFields(['id']))
                ->setRangePartitioning((new RangePartitioning())
                    ->setField('id')
                    ->setRange((new RangePartitioning\Range())
                        ->setStart('0')
                        ->setEnd('10')
                        ->setInterval('1'))),
            'range',
        );

        $this->assertNotNull($tableInfo->getMeta());
        $meta = $tableInfo->getMeta()->unpack();
        $this->assertInstanceOf(TableInfo\BigQueryTableMeta::class, $meta);
        $this->assertNotNull($meta->getClustering());
        $this->assertSame(['id'], ProtobufHelper::repeatedStringToArray($meta->getClustering()->getFields()));
        $this->assertNull($meta->getTimePartitioning());
        $this->assertNotNull($meta->getRangePartitioning());
        $this->assertSame('id', $meta->getRangePartitioning()->getField());
        $this->assertNotNull($meta->getRangePartitioning()->getRange());
        $this->assertSame('0', $meta->getRangePartitioning()->getRange()->getStart());
        $this->assertSame('10', $meta->getRangePartitioning()->getRange()->getEnd());
        $this->assertSame('1', $meta->getRangePartitioning()->getRange()->getInterval());

        // Range partitioning and clustering
        $expirationMs = (string) (1000 * 60 * 60 * 24 * 10);
        $tableInfo = $this->createTableForPartitioning(
            (new CreateTableCommand\BigQueryTableMeta())
                ->setClustering((new Clustering())->setFields(['id']))
                ->setTimePartitioning((new TimePartitioning())
                    ->setType('DAY')
                    ->setField('time')
                    ->setExpirationMs($expirationMs)/**10 days*/),
            'time',
        );

        $this->assertNotNull($tableInfo->getMeta());
        $meta = $tableInfo->getMeta()->unpack();
        $this->assertInstanceOf(TableInfo\BigQueryTableMeta::class, $meta);
        $this->assertNotNull($meta->getClustering());
        $this->assertSame(['id'], ProtobufHelper::repeatedStringToArray($meta->getClustering()->getFields()));
        $this->assertNull($meta->getRangePartitioning());
        $this->assertNotNull($meta->getTimePartitioning());
        $this->assertSame('DAY', $meta->getTimePartitioning()->getType());
        $this->assertSame('time', $meta->getTimePartitioning()->getField());
        $this->assertSame($expirationMs, $meta->getTimePartitioning()->getExpirationMs());
    }

    public function testCreateTableFail(): void
    {
        $this->expectException(BadTableDefinitionException::class);
        $this->expectExceptionMessage('Failed to create table');
        $this->createTableForPartitioning(
            (new CreateTableCommand\BigQueryTableMeta())
                // range partitioning must have range defined
                ->setRangePartitioning((new RangePartitioning())
                    ->setField('id')
                    ->setRange((new RangePartitioning\Range()))),
            'range',
        );
    }

    public function testCreateTableFailOnInvalidLength(): void
    {
        $tableName = md5($this->getName());
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setNullable(false)
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = (new TableColumnShared)
            ->setName('time')
            ->setType(Bigquery::TYPE_STRUCT)
            ->setLength('x x x');

        $this->expectException(BadTableDefinitionException::class);
        $this->expectExceptionMessage('Failed to create table');
        $handler(
            $this->projectCredentials,
            (new CreateTableCommand())
                ->setPath($path)
                ->setTableName($tableName)
                ->setColumns($columns),
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    private function createTableForPartitioning(
        CreateTableCommand\BigQueryTableMeta $meta,
        string $nameSuffix,
    ): TableInfo {
        $tableName = md5($this->getName()) . $nameSuffix;
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setNullable(false)
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = (new TableColumnShared)
            ->setName('time')
            ->setType(Bigquery::TYPE_TIMESTAMP)
            ->setNullable(false);
        $any = new Any();
        $any->pack($meta);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns)
            ->setMeta($any);
        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertNotNull($response->getTableInfo());
        return $response->getTableInfo();
    }

    /** @dataProvider failsDefaultTypesProvider */
    public function testFailsDefaultsCreateTable(TableColumnShared $column): void
    {
        $tableName = $this->getTestHash() . '_Test_table';
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = $column;
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns);

        try {
            $handler(
                $this->projectCredentials,
                $command,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('Should fail');
        } catch (BadTableDefinitionException $e) {
            $this->assertStringContainsString('Invalid default value expression for column', $e->getMessage());
        }
    }

    public function failsDefaultTypesProvider(): Generator
    {
        // INT64
        yield Bigquery::TYPE_INT64 . '_fail' => [
            (new TableColumnShared)
                ->setName('int64')
                ->setType(Bigquery::TYPE_INT64)
                ->setDefault('fail'),
        ];

        // BYTES
        yield Bigquery::TYPE_BYTES . '_fail' => [
            (new TableColumnShared)
                ->setName('bytes')
                ->setType(Bigquery::TYPE_BYTES)
                ->setDefault('fail'),
        ];

        // NUMERIC
        yield Bigquery::TYPE_NUMERIC . '_fail' => [
            (new TableColumnShared)
                ->setName('numeric')
                ->setType(Bigquery::TYPE_NUMERIC)
                ->setDefault('fail'),
        ];

        // NUMERIC
        yield Bigquery::TYPE_BIGNUMERIC . '_fail' => [
            (new TableColumnShared)
                ->setName('bignumeric')
                ->setType(Bigquery::TYPE_BIGNUMERIC)
                ->setDefault('fail'),
        ];

        // FLOAT64
        yield Bigquery::TYPE_FLOAT64 . '_fail' => [
            (new TableColumnShared)
                ->setName('float64')
                ->setType(Bigquery::TYPE_FLOAT64)
                ->setDefault('fail'),
        ];

        // STRING
        yield Bigquery::TYPE_STRING . '_fail' => [
            (new TableColumnShared)
                ->setName('string')
                ->setType(Bigquery::TYPE_STRING)
                ->setDefault('1'),
        ];

        // BOOL
        yield Bigquery::TYPE_BOOL . '_fail_1' => [
            (new TableColumnShared)
                ->setName('bool')
                ->setType(Bigquery::TYPE_BOOL)
                ->setDefault('test'),
        ];

         // DATE
        yield Bigquery::TYPE_DATE . '_fail' => [
            (new TableColumnShared)
                ->setName('date')
                ->setType(Bigquery::TYPE_DATE)
                ->setDefault('fail'),
        ];

        // DATETIME
        yield Bigquery::TYPE_DATETIME . '_fail' => [
            (new TableColumnShared)
                ->setName('datetime')
                ->setType(Bigquery::TYPE_DATETIME)
                ->setDefault('fail'),
        ];

        // TIME
        yield Bigquery::TYPE_TIME . '_fail' => [
            (new TableColumnShared)
                ->setName('time')
                ->setType(Bigquery::TYPE_TIME)
                ->setDefault('fail'),
        ];

        // TIMESTAMP
        yield Bigquery::TYPE_TIMESTAMP . '_fail' => [
            (new TableColumnShared)
                ->setName('timestamp')
                ->setType(Bigquery::TYPE_TIMESTAMP)
                ->setDefault('fail'),
        ];
    }

    /** @dataProvider defaultTypesProvider */
    public function testDefaultsCreateTable(TableColumnShared $column): void
    {
        $tableName = $this->getTestHash() . '_Test_table';
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = $column;
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns);
        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertNotNull($response->getTableInfo());
    }

    public function defaultTypesProvider(): Generator
    {
        // INT64
        yield Bigquery::TYPE_INT64 . '_string' => [
            (new TableColumnShared)
                ->setName('int64')
                ->setType(Bigquery::TYPE_INT64)
                ->setDefault('1'),
        ];

        yield Bigquery::TYPE_INT64 . '_empty' => [
            (new TableColumnShared)
                ->setName('int64')
                ->setType(Bigquery::TYPE_INT64)
                ->setDefault(''),
        ];

        yield Bigquery::TYPE_BYTES . '_string' => [
            (new TableColumnShared)
                ->setName('bytes')
                ->setType(Bigquery::TYPE_BYTES)
                ->setDefault('B"abc"'),
        ];

        yield Bigquery::TYPE_BYTES . '_empty' => [
            (new TableColumnShared)
                ->setName('bytes')
                ->setType(Bigquery::TYPE_BYTES)
                ->setDefault(''),
        ];

        // NUMERIC
        yield Bigquery::TYPE_NUMERIC . '_string' => [
            (new TableColumnShared)
                ->setName('numeric')
                ->setType(Bigquery::TYPE_NUMERIC)
                ->setDefault('1'),
        ];

        yield Bigquery::TYPE_NUMERIC . '_empty' => [
            (new TableColumnShared)
                ->setName('numeric')
                ->setType(Bigquery::TYPE_NUMERIC)
                ->setDefault(''),
        ];

        // NUMERIC
        yield Bigquery::TYPE_BIGNUMERIC . '_string' => [
            (new TableColumnShared)
                ->setName('bignumeric')
                ->setType(Bigquery::TYPE_BIGNUMERIC)
                ->setDefault('1'),
        ];

        yield Bigquery::TYPE_BIGNUMERIC . '_empty' => [
            (new TableColumnShared)
                ->setName('bignumeric')
                ->setType(Bigquery::TYPE_BIGNUMERIC)
                ->setDefault(''),
        ];

        // FLOAT64
        yield Bigquery::TYPE_FLOAT64 . '_string' => [
            (new TableColumnShared)
                ->setName('float64')
                ->setType(Bigquery::TYPE_FLOAT64)
                ->setDefault('1'),
        ];

        yield Bigquery::TYPE_FLOAT64 . '_empty' => [
            (new TableColumnShared)
                ->setName('float64')
                ->setType(Bigquery::TYPE_FLOAT64)
                ->setDefault(''),
        ];

        yield Bigquery::TYPE_STRING . '_string' => [
            (new TableColumnShared)
                ->setName('string')
                ->setType(Bigquery::TYPE_STRING)
                ->setDefault('\'roman\''),
        ];

        yield Bigquery::TYPE_STRING . '_empty' => [
            (new TableColumnShared)
                ->setName('string')
                ->setType(Bigquery::TYPE_STRING)
                ->setDefault(''),
        ];

        // BOOL
        yield Bigquery::TYPE_BOOL . '_bool_string_true' => [
            (new TableColumnShared)
                ->setName('bool')
                ->setType(Bigquery::TYPE_BOOL)
                ->setDefault('true'),
        ];

        yield Bigquery::TYPE_BOOL . '_bool_string_false' => [
            (new TableColumnShared)
                ->setName('bool')
                ->setType(Bigquery::TYPE_BOOL)
                ->setDefault('false'),
        ];

        yield Bigquery::TYPE_BOOL . '_bool_empty' => [
            (new TableColumnShared)
                ->setName('bool')
                ->setType(Bigquery::TYPE_BOOL)
                ->setDefault(''),
        ];

        // DATE
        yield Bigquery::TYPE_DATE . '_empty' => [
            (new TableColumnShared)
                ->setName('date')
                ->setType(Bigquery::TYPE_DATE)
                ->setDefault(''),
        ];

        yield Bigquery::TYPE_DATE . '_qouted' => [
            (new TableColumnShared)
                ->setName('date')
                ->setType(Bigquery::TYPE_DATE)
                ->setDefault('\'2022-02-22\''),
        ];

        // DATETIME
        yield Bigquery::TYPE_DATETIME . '_current' => [
            (new TableColumnShared)
                ->setName('datetime')
                ->setType(Bigquery::TYPE_DATETIME)
                ->setDefault('CURRENT_DATETIME()'),
        ];

        yield Bigquery::TYPE_DATETIME . '_empty' => [
            (new TableColumnShared)
                ->setName('datetime')
                ->setType(Bigquery::TYPE_DATETIME)
                ->setDefault(''),
        ];

        yield Bigquery::TYPE_DATETIME . '_quoted' => [
            (new TableColumnShared)
                ->setName('datetime')
                ->setType(Bigquery::TYPE_DATETIME)
                ->setDefault('\'2021-01-01 00:00:00\''),
        ];

        // TIME
        yield Bigquery::TYPE_TIME . '_method' => [
            (new TableColumnShared)
                ->setName('time')
                ->setType(Bigquery::TYPE_TIME)
                ->setDefault('current_time()'),
        ];

        yield Bigquery::TYPE_TIME . '_empty' => [
            (new TableColumnShared)
                ->setName('time')
                ->setType(Bigquery::TYPE_TIME)
                ->setDefault(''),
        ];

        yield Bigquery::TYPE_TIME . '_quoted' => [
            (new TableColumnShared)
                ->setName('time')
                ->setType(Bigquery::TYPE_TIME)
                ->setDefault('\'00:00:00\''),
        ];

        // TIMESTAMP
        yield Bigquery::TYPE_TIMESTAMP . '_method' => [
            (new TableColumnShared)
                ->setName('timestamp')
                ->setType(Bigquery::TYPE_TIMESTAMP)
                ->setDefault('current_timestamp()'),
        ];

        yield Bigquery::TYPE_TIMESTAMP . '_empty' => [
            (new TableColumnShared)
                ->setName('timestamp')
                ->setType(Bigquery::TYPE_TIMESTAMP)
                ->setDefault(''),
        ];

        yield Bigquery::TYPE_TIMESTAMP . '_quoted' => [
            (new TableColumnShared)
                ->setName('timestamp')
                ->setType(Bigquery::TYPE_TIMESTAMP)
                ->setDefault('\'2021-01-01 00:00:00\''),
        ];
    }
}
