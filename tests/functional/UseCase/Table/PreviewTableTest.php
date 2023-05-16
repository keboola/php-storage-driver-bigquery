<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Generator;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\BadExportFilterParameters;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Preview\PreviewTableHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportFilters;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
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
                'decimal_varchar' => [
                    'type' => Bigquery::TYPE_STRING,
                    'length' => '10',
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
                    'nullable' => true,
                ],
                'datetime' => [
                    'type' => Bigquery::TYPE_DATETIME,
                    'nullable' => true,
                ],
            ],
            'primaryKeysNames' => ['id'],
        ];
        $this->createTable($this->projectCredentials, $bucketDatabaseName, $tableName, $tableStructure);

        // FILL DATA
        $insertGroups = [
            [
                //phpcs:ignore
                'columns' => '`id`, `int`, `decimal`,`decimal_varchar`, `float`, `date`, `time`, `_timestamp`, `varchar`, `datetime`',
                'rows' => [
                    //phpcs:ignore
                    "1, 100, 100.23, '100.23', 100.23456, '2022-01-01', '12:00:02', '2022-01-01 12:00:02', 'Variable character 1', '1989-08-31 00:00:00'",
                    // chanched `time` and `varchar`
                    //phpcs:ignore
                    "2, 100, 100.23, '100.20', 100.23456, '2022-01-01', '12:00:10', '2022-01-01 12:00:10', 'Variable 2', '1989-08-31 01:00:00.123456'",
                    sprintf(
                    //phpcs:ignore
                        "3, 200, 200.23, '200.23', 200.23456, '2022-01-02', '12:00:10', '2022-01-01 12:00:10', '%s', '1989-08-31 02:00:00'",
                        str_repeat('VeryLongString123456', 5)
                    ),
                    '4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
                ],
            ],
        ];
        $this->fillTableWithData($this->projectCredentials, $bucketDatabaseName, $tableName, $insertGroups);

        // CHECK: all records + truncated
        $filter = [
            'input' => [
                'columns' => ['id', 'int', 'decimal', 'float', 'date', 'time', '_timestamp', 'varchar', 'datetime'],
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
                    ]),
                ],
            ],
            'expectedColumns' => ['id', 'int', 'decimal', 'float', 'date', 'time', '_timestamp', 'varchar', 'datetime'],
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
                        'value' => ['string_value' => '12:00:02.000000'],
                        'truncated' => false,
                    ],
                    '_timestamp' => [
                        'value' => ['string_value' => '2022-01-01 12:00:02.000000+00:00'],
                        'truncated' => false,
                    ],
                    'varchar' => [
                        'value' => ['string_value' => 'Variable character 1'],
                        'truncated' => false,
                    ],
                    'datetime' => [
                        'value' => ['string_value' => '1989-08-31 00:00:00.000000'],
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
                        'value' => ['string_value' => '12:00:10.000000'],
                        'truncated' => false,
                    ],
                    '_timestamp' => [
                        'value' => ['string_value' => '2022-01-01 12:00:10.000000+00:00'],
                        'truncated' => false,
                    ],
                    'varchar' => [
                        'value' => ['string_value' => 'Variable 2'],
                        'truncated' => false,
                    ],
                    'datetime' => [
                        'value' => ['string_value' => '1989-08-31 01:00:00.123456'],
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
                        'value' => ['string_value' => '12:00:10.000000'],
                        'truncated' => false,
                    ],
                    '_timestamp' => [
                        'value' => ['string_value' => '2022-01-01 12:00:10.000000+00:00'],
                        'truncated' => false,
                    ],
                    'varchar' => [
                        //phpcs:ignore
                        'value' => ['string_value' => 'VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456'],
                        'truncated' => false,
                    ],
                    'datetime' => [
                        //phpcs:ignore
                        'value' => ['string_value' => '1989-08-31 02:00:00.000000'],
                        'truncated' => false,
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
                    'datetime' => [
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
                    new ExportOrderBy([
                        'columnName' => 'time',
                        'order' => ExportOrderBy\Order::DESC,
                    ]),
                    new ExportOrderBy([
                        'columnName' => 'int',
                        'order' => ExportOrderBy\Order::ASC,
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
                    new ExportOrderBy([
                        'columnName' => 'date',
                        'order' => ExportOrderBy\Order::ASC,
                        'dataType' => DataType::STRING,
                    ]),
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
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
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
                    ]),
                ],
                'filters' => new ExportFilters([
                    'limit' => 2,
                ]),
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

        // CHECK: changeSince + changeUntil
        $filter = [
            'input' => [
                'columns' => ['id', '_timestamp'],
                'filters' => new ExportFilters([
                    'changeSince' => '1641038401',
                    'changeUntil' => '1641038403',
                ]),
            ],
            'expectedColumns' => ['id', '_timestamp'],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '1'],
                        'truncated' => false,
                    ],
                    '_timestamp' => [
                        'value' => ['string_value' => '2022-01-01 12:00:02.000000+00:00'],
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
                'filters' => new ExportFilters([
                    'fulltextSearch' => 'character',
                ]),
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
                'filters' => new ExportFilters([
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'int',
                            'operator' => Operator::ge,
                            'values' => ['100'],
                            'dataType' => DataType::INTEGER,
                        ]),
                    ],
                ]),
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
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
                        'value' => ['string_value' => '100'],
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
                ],
            ],
        ];
        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

        // CHECK: multiple where filters
        $filter = [
            'input' => [
                'columns' => ['id', 'int'],
                'filters' => new ExportFilters([
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'int',
                            'operator' => Operator::gt,
                            'values' => ['100'],
                            'dataType' => DataType::INTEGER,
                        ]),
                        new TableWhereFilter([
                            'columnsName' => 'int',
                            'operator' => Operator::lt,
                            'values' => ['210'],
                            'dataType' => DataType::INTEGER,
                        ]),
                        new TableWhereFilter([
                            'columnsName' => 'int',
                            'operator' => Operator::eq,
                            'values' => ['99', '100', '199', '200'],
                            'dataType' => DataType::INTEGER,
                        ]),
                    ],
                ]),
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
                    ]),
                ],
            ],
            'expectedColumns' => ['id', 'int'],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '3'],
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
                'filters' => new ExportFilters([
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'decimal_varchar',
                            'operator' => Operator::eq,
                            'values' => ['100.2'],
                            'dataType' => DataType::REAL,
                        ]),
                    ],
                ]),
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
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

        // FILL DATA
        $insertGroups = [
            [
                //phpcs:ignore
                'columns' => '`id`, `int`, `decimal`,`decimal_varchar`, `float`, `date`, `time`, `_timestamp`, `varchar`',
                'rows' => [
                    //phpcs:ignore
                    sprintf(
                        "5, 200, 200.23, '200.23', 200.23456, '2022-01-02', '12:00:10', '2022-01-01 12:00:10', '%s'",
                        str_repeat('VeryLongString123456', 1000)
                    ),
                ],
            ],
        ];
        $this->fillTableWithData($this->projectCredentials, $bucketDatabaseName, $tableName, $insertGroups);

        // CHECK: check truncate
        $filter = [
            'input' => [
                'columns' => [
                    'id',
                    'varchar',
                ],
                'filters' => new ExportFilters([
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'id',
                            'operator' => Operator::eq,
                            'values' => ['5'],
                            'dataType' => DataType::INTEGER,
                        ]),
                    ],
                ]),
            ],
            'expectedColumns' => [
                'id',
                'varchar',
            ],
            'expectedRows' => [
                [
                    'id' => [
                        'value' => ['string_value' => '5'],
                        'truncated' => false,
                    ],
                    'varchar' => [
                        //phpcs:ignore
                        'value' => ['string_value' => 'VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456Very'],
                        'truncated' => true,
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
                'filters' => new ExportFilters([
                    'limit' => 2000,
                ]),
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
                'filters' => new ExportFilters([
                    'changeSince' => '2022-11-01 12:00:00 UTC',
                ]),
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
                'filters' => new ExportFilters([
                    'changeUntil' => '2022-11-01 12:00:00 UTC',
                ]),
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
                    new ExportOrderBy([
                        'columnName' => '',
                        'order' => ExportOrderBy\Order::ASC,
                    ]),
                ],
            ]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString('PreviewTableCommand.orderBy.0.columnName is required', $e->getMessage());
        }
    }

    /**
     * @phpcs:ignore
     * @param array{
     *     columns: array<string>,
     *     orderBy?: ExportOrderBy[],
     *     filters?: ExportFilters
     * } $params
     * @dataProvider  filterProvider
     */
    public function testTablePreviewWithWrongTypesInWhereFilters(array $params, string $expectExceptionMessage): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'int' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => true,
                ],
                'date' => [
                    'type' => Bigquery::TYPE_DATE,
                    'length' => '',
                    'nullable' => true,
                ],
                'datetime' => [
                    'type' => Bigquery::TYPE_DATETIME,
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
                'timestamp' => [
                    'type' => Bigquery::TYPE_TIMESTAMP,
                    'length' => '',
                    'nullable' => false,
                ],
            ],
            'primaryKeysNames' => [],
        ];
        $this->createTable($this->projectCredentials, $bucketDatabaseName, $tableName, $tableStructure);

        // FILL DATA
        $insertGroups = [
            [
                'columns' => '`int`, `date`, `datetime`, `time`, `varchar`, `timestamp`',
                'rows' => [
                    "200, '2022-01-01', '2022-01-01 12:00:02', '12:35:00', 'xxx', '1989-08-31 00:00:00.000'",
                ],
            ],
        ];
        $this->fillTableWithData($this->projectCredentials, $bucketDatabaseName, $tableName, $insertGroups);

        try {
            $this->previewTable($bucketDatabaseName, $tableName, $params);
            $this->fail('This should never happen');
        } catch (BadExportFilterParameters $e) {
            $this->assertStringContainsString($expectExceptionMessage, $e->getMessage());
        }
    }

    public function filterProvider(): Generator
    {
        yield 'wrong int' => [
            [
                'columns' => ['int'],
                'filters' => new ImportExportShared\ExportFilters([
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'int',
                            'operator' => Operator::eq,
                            'values' => ['aaa'],
                        ]),
                    ],
                ]),
            ],
            'Invalid filter value, expected:"INT64", actual:"STRING".',
        ];

        yield 'wrong date' => [
            [
                'columns' => ['date'],
                'filters' => new ImportExportShared\ExportFilters([
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'date',
                            'operator' => Operator::eq,
                            'values' => ['2022-02-31'],
                        ]),
                    ],
                ]),
            ],
            // non-existing date
            'Invalid date: \'2022-02-31\'; while executing the filter on column \'date\'; Column \'date\'',
        ];

        yield 'wrong time' => [
            [
                'columns' => ['time'],
                'filters' => new ImportExportShared\ExportFilters([
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'time',
                            'operator' => Operator::eq,
                            'values' => ['25:59:59.999999'],
                        ]),
                    ],
                ]),
            ],
            'Invalid time string "25:59:59.999999"; while executing the filter on column \'time\'; Column \'time\'',
        ];

        yield 'wrong timestamp' => [
            [
                'columns' => ['timestamp'],
                'filters' => new ImportExportShared\ExportFilters([
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'timestamp',
                            'operator' => Operator::eq,
                            'values' => ['25:59:59.999999'],
                        ]),
                    ],
                ]),
            ],
            //phpcs:ignore
            "Invalid timestamp: '25:59:59.999999'; while executing the filter on column 'timestamp'; Column 'timestamp'",
        ];

        yield 'wrong more filters' => [
            [
                'columns' => ['int'],
                'filters' => new ImportExportShared\ExportFilters([
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'int',
                            'operator' => Operator::lt,
                            'values' => ['aaa'],
                        ]),
                        new TableWhereFilter([
                            'columnsName' => 'int',
                            'operator' => Operator::gt,
                            'values' => ['aaa'],
                        ]),
                        new TableWhereFilter([
                            'columnsName' => 'time',
                            'operator' => Operator::eq,
                            'values' => ['25:59:59.999999'],
                        ]),
                    ],
                ]),
            ],
            'Invalid filter value, expected:"INT64", actual:"STRING".',
        ];
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
     *     orderBy?: ExportOrderBy[],
     *     filters?: ExportFilters
     * } $commandInput
     */
    private function previewTable(string $databaseName, string $tableName, array $commandInput): PreviewTableResponse
    {
        $handler = new PreviewTableHandler($this->clientManager);

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
        if (array_key_exists('filters', $commandInput)) {
            $command->setFilters($commandInput['filters']);
        }

        if (isset($commandInput['orderBy'])) {
            $orderBy = new RepeatedField(GPBType::MESSAGE, ExportOrderBy::class);
            foreach ($commandInput['orderBy'] as $orderByOrig) {
                $orderBy[] = $orderByOrig;
            }
            $command->setOrderBy($orderBy);
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
