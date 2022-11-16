<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
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
        $columns = new RepeatedField(GPBType::MESSAGE, CreateTableCommand\TableColumn::class);
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('id')
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('name')
            ->setType(Bigquery::TYPE_STRING)
            ->setLength('50')
            ->setNullable(true)
            ->setDefault("'Some Default'");
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('large')
            ->setType(Bigquery::TYPE_BIGNUMERIC)
            ->setLength('76,38')
            ->setDefault('185.554');
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('ordes')
            ->setType(Bigquery::TYPE_ARRAY)
            ->setLength('STRUCT<x ARRAY<STRUCT<xz ARRAY<INT64>>>>');
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('organization')
            ->setType(Bigquery::TYPE_STRUCT)
            ->setLength('x ARRAY<INT64>');
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns);
        $response = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertNull($response);

        $table = new BigqueryTableReflection(
            $this->clientManager->getBigQueryClient($this->projectCredentials),
            $bucketDatasetName,
            $tableName
        );

        /** @var BigqueryColumn[] $columns */
        $columns = iterator_to_array($table->getColumnsDefinitions());
        $this->assertCount(5, $columns);

        // check column ID
        $column = $columns[0];
        $this->assertSame('id', $column->getColumnName());
        $columnDef = $column->getColumnDefinition();
        $this->assertSame(Bigquery::TYPE_INT64, $columnDef->getType());
        $this->assertFalse($columnDef->isNullable());
        $this->assertNull($columnDef->getDefault());

        // check column NAME
        $column = $columns[1];
        $this->assertSame('name', $column->getColumnName());
        $columnDef = $column->getColumnDefinition();
        $this->assertSame(Bigquery::TYPE_STRING, $columnDef->getType());
        $this->assertSame('50', $columnDef->getLength());
        $this->assertTrue($columnDef->isNullable());
        $this->assertSame("'Some Default'", $columnDef->getDefault());

        // check column LARGE
        $column = $columns[2];
        $this->assertSame('large', $column->getColumnName());
        $columnDef = $column->getColumnDefinition();
        $this->assertSame(Bigquery::TYPE_BIGNUMERIC, $columnDef->getType());
        $this->assertSame('76,38', $columnDef->getLength());
        $this->assertFalse($columnDef->isNullable());
        $this->assertSame('185.554', $columnDef->getDefault());

        // check column array
        $column = $columns[3];
        $this->assertSame('ordes', $column->getColumnName());
        $columnDef = $column->getColumnDefinition();
        $this->assertSame(Bigquery::TYPE_ARRAY, $columnDef->getType());
        $this->assertSame('STRUCT<x ARRAY<STRUCT<xz ARRAY<INT64>>>>', $columnDef->getLength());
        $this->assertFalse($columnDef->isNullable());

        // check column array
        $column = $columns[4];
        $this->assertSame('organization', $column->getColumnName());
        $columnDef = $column->getColumnDefinition();
        $this->assertSame(Bigquery::TYPE_STRUCT, $columnDef->getType());
        $this->assertSame('x ARRAY<INT64>', $columnDef->getLength());
        $this->assertTrue($columnDef->isNullable());

        // DROP TABLE
        $handler = new DropTableHandler($this->clientManager);
        $command = (new DropTableCommand())
            ->setPath($path)
            ->setTableName($tableName);

        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);

        $bucket = $bqClient->dataset($bucketDatasetName);
        $this->assertTrue($bucket->exists());

        $table = $bucket->table($tableName);
        $this->assertFalse($table->exists());
    }
}
