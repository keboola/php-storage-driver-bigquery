<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
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
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

class CreateDropTableTest extends BaseCase
{
    protected CreateBucketResponse $bucketResponse;

    private GenericBackendCredentials $projectCredentials;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        $this->bucketResponse = $this->createTestBucket($projectCredentials);
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testCreateTable(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);

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
            new RuntimeOptions(),
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
        $this->assertSame(Bigquery::TYPE_INT64, $column->getType());
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
        $this->assertSame('STRUCT<x ARRAY<STRUCT<xz ARRAY<INT64>>>>', $column->getLength());
        $this->assertFalse($column->getNullable());

        // check column array
        /** @var TableInfo\TableColumn $column */
        $column = $columns[4];
        $this->assertSame('organization', $column->getName());
        $this->assertSame(Bigquery::TYPE_STRUCT, $column->getType());
        $this->assertSame('x ARRAY<INT64>', $column->getLength());
        $this->assertTrue($column->getNullable());

        // DROP TABLE
        $handler = new DropTableHandler($this->clientManager);
        $command = (new DropTableCommand())
            ->setPath($path)
            ->setTableName($tableName);

        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);

        $bucket = $bqClient->dataset($bucketDatasetName);
        $this->assertTrue($bucket->exists());

        $table = $bucket->table($tableName);
        $this->assertFalse($table->exists());
    }
}
