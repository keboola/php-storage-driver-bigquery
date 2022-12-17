<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Preview\PreviewTableHandler;
use Keboola\StorageDriver\BigQuery\QueryBuilder\TablePreviewFilterQueryBuilderFactory;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy\Order;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Throwable;

class PreviewTableTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    private TablePreviewFilterQueryBuilderFactory $tablePreviewFilterQueryBuilderFactory;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        $this->bucketResponse = $this->createTestBucket($projectCredentials);
        $this->tablePreviewFilterQueryBuilderFactory = new TablePreviewFilterQueryBuilderFactory();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testPreviewTable(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'id' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'int' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => true,
                ],
                'decimal' => [
                    'type' => Bigquery::TYPE_DECIMAL,
                    'length' => '10,2',
                    'nullable' => true,
                ],
                'float' => [
                    'type' => Bigquery::TYPE_FLOAT64,
                    'length' => '',
                    'nullable' => true,
                ],
                'date' => [
                    'type' => Bigquery::TYPE_DATE,
                    'length' => '',
                    'nullable' => true,
                ],
                'time' => [
                    'type' => Bigquery::TYPE_TIME,
                    'length' => '',
                    'nullable' => true,
                ],
                '_timestamp' => [
                    'type' => Bigquery::TYPE_TIMESTAMP,
                    'length' => '',
                    'nullable' => true,
                ],
                'varchar' => [
                    'type' => Bigquery::TYPE_STRING,
                    'length' => '200',
                    'nullable' => true,
                ],
            ],
            'primaryKeysNames' => ['id'],
        ];
        $this->createTable($this->projectCredentials, $bucketDatabaseName, $tableName, $tableStructure);

        // FILL DATA
        $insertGroups = [
            [
                'columns' => '`id`, `int`, `decimal`, `float`, `date`, `time`, `_timestamp`, `varchar`',
                'rows' => [
                    "1, 100, 100.23, 100.23456, '2022-01-01', '12:00:01', '2022-01-01 12:00:01', 'Variable character 1'",
                    // chanched `time` and `varchar`
                    "2, 100, 100.23, 100.23456, '2022-01-01', '12:00:02', '2022-01-01 12:00:01', 'Variable character 2'",
                    sprintf(
                        "3, 200, 200.23, 200.23456, '2022-01-02', '12:00:02', '2022-01-01 12:00:01', '%s'",
                        str_repeat('VeryLongString123456', 5)
                    ),
                    '4, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
                ],
            ],
        ];
        $this->fillTableWithData($this->projectCredentials, $bucketDatabaseName, $tableName, $insertGroups);

        // CHECK: all records + truncated
        $filter = [
            'input' => [
                'columns' => ['id', 'int', 'decimal', 'float', 'date', 'time', '_timestamp', 'varchar'],
                'orderBy' => [
                    new OrderBy([
                        'columnName' => 'id',
                        'order' => Order::ASC,
                    ]),
                ],
            ],
            'expectedColumns' => ['id', 'int', 'decimal', 'float', 'date', 'time', '_timestamp', 'varchar'],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '1'],
                        'truncated' => false,
                    ],
                    'int' => [
                        'value' => ['string_value' => '100'],
                        'truncated' => false,
                    ],
                    'decimal' => [
                        'value' => ['string_value' => '100.23'],
                        'truncated' => false,
                    ],
                    'float' => [
                        'value' => ['string_value' => '100.23456'],
                        'truncated' => false,
                    ],
                    'date' => [
                        'value' => ['string_value' => '2022-01-01'],
                        'truncated' => false,
                    ],
                    'time' => [
                        'value' => ['string_value' => '12:00:01.000000'],
                        'truncated' => false,
                    ],
                    '_timestamp' => [
                        'value' => ['string_value' => '2022-01-01 12:00:01.000000+00:00'],
                        'truncated' => false,
                    ],
                    'varchar' => [
                        'value' => ['string_value' => 'Variable character 1'],
                        'truncated' => false,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '2'],
                        'truncated' => false,
                    ],
                    'int' => [
                        'value' => ['string_value' => '100'],
                        'truncated' => false,
                    ],
                    'decimal' => [
                        'value' => ['string_value' => '100.23'],
                        'truncated' => false,
                    ],
                    'float' => [
                        'value' => ['string_value' => '100.23456'],
                        'truncated' => false,
                    ],
                    'date' => [
                        'value' => ['string_value' => '2022-01-01'],
                        'truncated' => false,
                    ],
                    'time' => [
                        'value' => ['string_value' => '12:00:02.000000'],
                        'truncated' => false,
                    ],
                    '_timestamp' => [
                        'value' => ['string_value' => '2022-01-01 12:00:01.000000+00:00'],
                        'truncated' => false,
                    ],
                    'varchar' => [
                        'value' => ['string_value' => 'Variable character 2'],
                        'truncated' => false,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '3'],
                        'truncated' => false,
                    ],
                    'int' => [
                        'value' => ['string_value' => '200'],
                        'truncated' => false,
                    ],
                    'decimal' => [
                        'value' => ['string_value' => '200.23'],
                        'truncated' => false,
                    ],
                    'float' => [
                        'value' => ['string_value' => '200.23456'],
                        'truncated' => false,
                    ],
                    'date' => [
                        'value' => ['string_value' => '2022-01-02'],
                        'truncated' => false,
                    ],
                    'time' => [
                        'value' => ['string_value' => '12:00:02.000000'],
                        'truncated' => false,
                    ],
                    '_timestamp' => [
                        'value' => ['string_value' => '2022-01-01 12:00:01.000000+00:00'],
                        'truncated' => false,
                    ],
                    'varchar' => [
                        'value' => ['string_value' => 'VeryLongString123456VeryLongString123456VeryLongSt'],
                        'truncated' => true,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '4'],
                        'truncated' => false,
                    ],
                    'int' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'decimal' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'float' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'date' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'time' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    '_timestamp' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'varchar' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                ],
            ],
        ];
        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

        // CHECK: order by
        $filter = [
            'input' => [
                'columns' => ['id'],
                'orderBy' => [
                    new OrderBy([
                        'columnName' => 'time',
                        'order' => Order::DESC,
                    ]),
                    new OrderBy([
                        'columnName' => 'int',
                        'order' => Order::ASC,
                    ]),
                ],
            ],
            'expectedColumns' => ['id'],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '2'],
                        'truncated' => false,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '3'],
                        'truncated' => false,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '1'],
                        'truncated' => false,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '4'],
                        'truncated' => false,
                    ],
                ],
            ],
        ];
        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

        // CHECK: order by with dataType - null value is the first because of cast to string
        $filter = [
            'input' => [
                'columns' => ['id'],
                'orderBy' => [
                    new OrderBy([
                        'columnName' => 'date',
                        'order' => Order::ASC,
                        'dataType' => DataType::STRING,
                    ]),
                    new OrderBy([
                        'columnName' => 'id',
                        'order' => Order::ASC,
                    ]),
                ],
            ],
            'expectedColumns' => ['id'],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '4'],
                        'truncated' => false,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '1'],
                        'truncated' => false,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '2'],
                        'truncated' => false,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '3'],
                        'truncated' => false,
                    ],
                ],
            ],
        ];
        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

        // CHECK: limit
        $filter = [
            'input' => [
                'columns' => ['id', 'int'],
                'orderBy' => [
                    new OrderBy([
                        'columnName' => 'id',
                        'order' => Order::ASC,
                    ]),
                ],
                'limit' => 2,
            ],
            'expectedColumns' => ['id', 'int'],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '1'],
                        'truncated' => false,
                    ],
                    'int' => [
                        'value' => ['string_value' => '100'],
                        'truncated' => false,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '2'],
                        'truncated' => false,
                    ],
                    'int' => [
                        'value' => ['string_value' => '100'],
                        'truncated' => false,
                    ],
                ],
            ],
        ];
        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

        // CHECK: changedSince + changedUntil
        $filter = [
            'input' => [
                'columns' => ['id', '_timestamp'],
                'changedSince' => '1641038401',
                'changedUntil' => '1641038402',
            ],
            'expectedColumns' => ['id', '_timestamp'],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '1'],
                        'truncated' => false,
                    ],
                    '_timestamp' => [
                        'value' => ['string_value' => '2022-01-01 12:00:01.000000+00:00'],
                        'truncated' => false,
                    ],
                ],
            ],
        ];
        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

        // CHECK: fulltext search
        $filter = [
            'input' => [
                'columns' => ['id', 'varchar'],
                'fulltextSearch' => 'character',
            ],
            'expectedColumns' => ['id', 'varchar'],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '1'],
                        'truncated' => false,
                    ],
                    'varchar' => [
                        'value' => ['string_value' => 'Variable character 1'],
                        'truncated' => false,
                    ],
                ],
            ],
        ];
        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

        // CHECK: simple where filter
        $filter = [
            'input' => [
                'columns' => ['id', 'int'],
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'int',
                        'operator' => Operator::ge,
                        'values' => ['100'],
                    ]),
                ],
                'orderBy' => [
                    new OrderBy([
                        'columnName' => 'id',
                        'order' => Order::ASC,
                    ]),
                ],
            ],
            'expectedColumns' => ['id', 'int'],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '1'],
                        'truncated' => false,
                    ],
                    'int' => [
                        'value' => ['string_value' => '100'],
                        'truncated' => false,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '2'],
                        'truncated' => false,
                    ],
                    'int' => [
                        'value' => ['string_value' => '200'],
                        'truncated' => false,
                    ],
                ],
            ],
        ];
        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

        // CHECK: multiple where filters
        $filter = [
            'input' => [
                'columns' => ['id', 'int'],
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'int',
                        'operator' => Operator::gt,
                        'values' => ['100'],
                    ]),
                    new TableWhereFilter([
                        'columnsName' => 'int',
                        'operator' => Operator::lt,
                        'values' => ['210'],
                    ]),
                    new TableWhereFilter([
                        'columnsName' => 'int',
                        'operator' => Operator::eq,
                        'values' => ['99', '100', '199', '200'],
                    ]),
                ],
                'orderBy' => [
                    new OrderBy([
                        'columnName' => 'id',
                        'order' => Order::ASC,
                    ]),
                ],
            ],
            'expectedColumns' => ['id', 'int'],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '2'],
                        'truncated' => false,
                    ],
                    'int' => [
                        'value' => ['string_value' => '200'],
                        'truncated' => false,
                    ],
                ],
            ],
        ];
        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

        // CHECK: where filter with datatype
        $filter = [
            'input' => [
                'columns' => ['id', 'decimal_varchar'],
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'decimal_varchar',
                        'operator' => Operator::eq,
                        'values' => ['100.2'],
                        'dataType' => DataType::REAL,
                    ]),
                ],
                'orderBy' => [
                    new OrderBy([
                        'columnName' => 'id',
                        'order' => Order::ASC,
                    ]),
                ],
            ],
            'expectedColumns' => ['id', 'decimal_varchar'],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '2'],
                        'truncated' => false,
                    ],
                    'decimal_varchar' => [
                        'value' => ['string_value' => '100.20'],
                        'truncated' => false,
                    ],
                ],
            ],
        ];
        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

        // DROP TABLE
        $this->dropTable($bucketDatabaseName, $tableName);
    }

    public function testPreviewTableMissingArguments(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'id' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'int' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => true,
                ],
                'decimal' => [
                    'type' => Bigquery::TYPE_DECIMAL,
                    'length' => '10,2',
                    'nullable' => true,
                ],
                'float' => [
                    'type' => Bigquery::TYPE_FLOAT64,
                    'length' => '',
                    'nullable' => true,
                ],
                'date' => [
                    'type' => Bigquery::TYPE_DATE,
                    'length' => '',
                    'nullable' => true,
                ],
                'time' => [
                    'type' => Bigquery::TYPE_TIME,
                    'length' => '',
                    'nullable' => true,
                ],
                'varchar' => [
                    'type' => Bigquery::TYPE_STRING,
                    'length' => '200',
                    'nullable' => true,
                ],
            ],
            'primaryKeysNames' => ['id'],
        ];
        $this->createTable($this->projectCredentials, $bucketDatabaseName, $tableName, $tableStructure);

        // PREVIEW
        // empty path
        try {
            $this->previewTable('', $tableName, ['columns' => ['id', 'int']]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                'PreviewTableCommand.path is required and size must equal 1',
                $e->getMessage()
            );
        }

        // empty tableName
        try {
            $this->previewTable($bucketDatabaseName, '', ['columns' => ['id', 'int']]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString('PreviewTableCommand.tableName is required', $e->getMessage());
        }

        // empty list of columns
        try {
            $this->previewTable($bucketDatabaseName, $tableName, ['columns' => []]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString('PreviewTableCommand.columns is required', $e->getMessage());
        }

        // non unique values in columns
        try {
            $this->previewTable($bucketDatabaseName, $tableName, ['columns' => ['id', 'id', 'int']]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString('PreviewTableCommand.columns has non unique names', $e->getMessage());
        }

        // too high limit
        try {
            $this->previewTable($bucketDatabaseName, $tableName, [
                'columns' => ['id', 'int'],
                'limit' => 2000,
            ]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                'PreviewTableCommand.limit cannot be greater than 1000',
                $e->getMessage()
            );
        }

        // bad format of changeSince
        try {
            $this->previewTable($bucketDatabaseName, $tableName, [
                'columns' => ['id', 'int'],
                'changedSince' => '2022-11-01 12:00:00 UTC',
            ]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                'PreviewTableCommand.changeSince must be numeric timestamp',
                $e->getMessage()
            );
        }

        // bad format of changeUntil
        try {
            $this->previewTable($bucketDatabaseName, $tableName, [
                'columns' => ['id', 'int'],
                'changedUntil' => '2022-11-01 12:00:00 UTC',
            ]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                'PreviewTableCommand.changeUntil must be numeric timestamp',
                $e->getMessage()
            );
        }

        // empty order by columnName
        try {
            $this->previewTable($bucketDatabaseName, $tableName, [
                'columns' => ['id', 'int'],
                'orderBy' => [
                    new OrderBy([
                        'columnName' => '',
                        'order' => Order::ASC,
                    ]),
                ],
            ]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString('PreviewTableCommand.orderBy.0.columnName is required', $e->getMessage());
        }

        // wrong order by dataType
        try {
            $this->previewTable($bucketDatabaseName, $tableName, [
                'columns' => ['id', 'int'],
                'orderBy' => [
                    new OrderBy([
                        'columnName' => 'id',
                        'order' => Order::ASC,
                        'dataType' => DataType::DECIMAL,
                    ]),
                ],
            ]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                'Data type DECIMAL not recognized. Possible datatypes are',
            $e->getMessage());
        }
    }

    private function dropTable(string $databaseName, string $tableName): void
    {
        $handler = new DropTableHandler($this->clientManager);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $databaseName;
        $command = (new DropTableCommand())
            ->setPath($path)
            ->setTableName($tableName);

        $handler(
            $this->projectCredentials,
            $command,
            []
        );
    }

    /**
     * @phpcs:ignore
     * @param array{
     *     columns: array<string>,
     *     orderBy?: OrderBy[],
     *     limit?: int
     * } $commandInput
     */
    private function previewTable(string $databaseName, string $tableName, array $commandInput): PreviewTableResponse
    {
        $handler = new PreviewTableHandler($this->clientManager, $this->tablePreviewFilterQueryBuilderFactory);

        $command = new PreviewTableCommand();

        if ($databaseName) {
            $path = new RepeatedField(GPBType::STRING);
            $path[] = $databaseName;
            $command->setPath($path);
        }

        if ($tableName) {
            $command->setTableName($tableName);
        }

        $columns = new RepeatedField(GPBType::STRING);
        foreach ($commandInput['columns'] as $column) {
            $columns[] = $column;
        }
        $command->setColumns($columns);

        if (isset($commandInput['changedSince'])) {
            $command->setChangeSince($commandInput['changedSince']);
        }

        if (isset($commandInput['changedUntil'])) {
            $command->setChangeUntil($commandInput['changedUntil']);
        }

        if (isset($commandInput['fulltextSearch'])) {
            $command->setFulltextSearch($commandInput['fulltextSearch']);
        }

        if (isset($commandInput['whereFilters'])) {
            $whereFilters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
            foreach ($commandInput['whereFilters'] as $whereFilter) {
                $whereFilters[] = $whereFilter;
            }
            $command->setWhereFilters($whereFilters);
        }

        if (isset($commandInput['orderBy'])) {
            $orderBy = new RepeatedField(GPBType::MESSAGE, OrderBy::class);
            foreach ($commandInput['orderBy'] as $orderByOrig) {
                $orderBy[] = $orderByOrig;
            }
            $command->setOrderBy($orderBy);
        }

        if (isset($commandInput['limit'])) {
            $command->setLimit($commandInput['limit']);
        }

        $response = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertInstanceOf(PreviewTableResponse::class, $response);
        return $response;
    }

    /**
     * @param string[] $expectedColumns
     * @param array<string, array{value: array<string, mixed>, truncated: bool}>[] $expectedRows
     */
    private function checkPreviewData(PreviewTableResponse $response, array $expectedColumns, array $expectedRows): void
    {
        $columns = ProtobufHelper::repeatedStringToArray($response->getColumns());
        $this->assertEqualsArrays($expectedColumns, $columns);

        // check rows
        $this->assertCount(count($expectedRows), $response->getRows());
        /** @var PreviewTableResponse\Row[] $rows */
        $rows = $response->getRows();
        foreach ($rows as $rowNumber => $row) {
            /** @var array<string, array<string, mixed>> $expectedRow */
            $expectedRow = $expectedRows[$rowNumber];

            // check columns
            /** @var PreviewTableResponse\Row\Column[] $columns */
            $columns = $row->getColumns();
            $this->assertCount(count($expectedRow), $columns);

            foreach ($columns as $column) {
                /** @var array{value: array<string, scalar>, truncated: bool} $expectedColumnValue */
                $expectedColumn = $expectedRow[$column->getColumnName()];

                // check column value
                /** @var array<string, scalar> $expectedColumnValue */
                $expectedColumnValue = $expectedColumn['value'];
                /** @var Value $columnValue */
                $columnValue = $column->getValue();
                $columnValueKind = $columnValue->getKind();
                $this->assertSame(key($expectedColumnValue), $columnValueKind);
                // preview returns all data as string
                if ($columnValueKind === 'null_value') {
                    $this->assertTrue($columnValue->hasNullValue());
                    $this->assertSame(current($expectedColumnValue), $columnValue->getNullValue());
                } elseif ($columnValueKind === 'string_value') {
                    $this->assertTrue($columnValue->hasStringValue());
                    $this->assertSame(current($expectedColumnValue), $columnValue->getStringValue());
                } else {
                    $this->fail(sprintf(
                        "Unsupported value kind '%s' in row #%d and column '%s'",
                        $columnValueKind,
                        $rowNumber,
                        $column->getColumnName()
                    ));
                }

                // check column truncated
                $this->assertSame($expectedColumn['truncated'], $column->getIsTruncated());
            }
        }
    }
}
