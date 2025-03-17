<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Generator;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\Backend\BigQuery\Clustering;
use Keboola\StorageDriver\Backend\BigQuery\RangePartitioning;
use Keboola\StorageDriver\BigQuery\Handler\Table\BadExportFilterParametersException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Preview\PreviewTableHandler;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnNotFoundException;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportFilters;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
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
        $this->projectCredentials = $this->projects[0][0];

        $this->bucketResponse = $this->createTestBucket($this->projects[0][0]);
    }

    public function testArrayPreviewTable(): void
    {
        $tableName = $this->getTestHash() . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'id' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'array_int' => [
                    'type' => Bigquery::TYPE_ARRAY,
                    'length' => 'INT64',
                    'nullable' => true,
                ],
                'array_string' => [
                    'type' => Bigquery::TYPE_ARRAY,
                    'length' => 'STRING',
                    'nullable' => true,
                ],
                'array_struct_int_int' => [
                    'type' => Bigquery::TYPE_ARRAY,
                    'length' => 'STRUCT<a INT64, b INT64>',
                    'nullable' => true,
                ],
                'array_struct_array_int' => [
                    'type' => Bigquery::TYPE_ARRAY,
                    'length' => 'STRUCT<a ARRAY<INT64>>',
                    'nullable' => true,
                ],
                'array_struct_array-int_array-string' => [
                    'type' => Bigquery::TYPE_ARRAY,
                    'length' => 'STRUCT<a ARRAY<INT64>, b ARRAY<STRING>>',
                    'nullable' => true,
                ],
                'array_very_long_string' => [
                    'type' => Bigquery::TYPE_ARRAY,
                    'length' => 'STRING',
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
                'columns' => '`id`, `array_int`, `array_string`, `array_struct_int_int`, `array_struct_array_int`, `array_struct_array-int_array-string`, `array_very_long_string`',
                'rows' => [
                    //phpcs:ignore
                    "1, [1,2,3], ['ke', 'boo', 'la'], [STRUCT(1,2), STRUCT(3,4)], [STRUCT([1,2]), STRUCT([3,4])], [STRUCT([1,2], ['a', 'b']), STRUCT([3,4], ['c', 'd'])], ['ke', 'boo', 'la']",
                    //phpcs:ignore
                    "2, [4,5,6], ['ro', 'man'], [STRUCT(5,6), STRUCT(7,8)], [STRUCT([5,6]), STRUCT([7,8])], [STRUCT([5,6], ['e', 'f']), STRUCT([7,8], ['g', 'h'])], ['ke', 'boo', 'la']",
                    //phpcs:ignore
                    sprintf("3, [7,8,9], ['789', '456', '123'], [STRUCT(9,10), STRUCT(11,12)], [STRUCT([9,10]), STRUCT([11,12])], [STRUCT([9,10], ['i', 'j']), STRUCT([11,12], ['k', 'l'])], ['%s']", str_repeat('VeryLongString123456', 1000)),
                    '4, NULL, NULL, NULL, NULL, NULL, NULL',
                ],
            ],
        ];
        $this->fillTableWithData($this->projectCredentials, $bucketDatabaseName, $tableName, $insertGroups);

        // CHECK: all records + truncated
        $filter = [
            'input' => [
                //phpcs:ignore
                'columns' => [
                    'array_int',
                    'array_string',
                    'array_struct_int_int',
                    'array_struct_array_int',
                    'array_struct_array-int_array-string',
                    'array_very_long_string',
                ],
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
                    ]),
                ],
            ],
            //phpcs:ignore
            'expectedColumns' => [
                'array_int',
                'array_string',
                'array_struct_int_int',
                'array_struct_array_int',
                'array_struct_array-int_array-string',
                'array_very_long_string',
            ],
            'expectedRows' => [
                [
                    'array_int' => [
                        'value' => ['string_value' => '[1,2,3]'],
                        'truncated' => false,
                    ],
                    'array_string' => [
                        'value' => ['string_value' => '["ke","boo","la"]'],
                        'truncated' => false,
                    ],
                    'array_struct_int_int' => [
                        'value' => ['string_value' => '[{"a":1,"b":2},{"a":3,"b":4}]'],
                        'truncated' => false,
                    ],
                    'array_struct_array_int' => [
                        'value' => ['string_value' => '[{"a":[1,2]},{"a":[3,4]}]'],
                        'truncated' => false,
                    ],
                    'array_struct_array-int_array-string' => [
                        'value' => ['string_value' => '[{"a":[1,2],"b":["a","b"]},{"a":[3,4],"b":["c","d"]}]'],
                        'truncated' => false,
                    ],
                    'array_very_long_string' => [
                        'value' => ['string_value' => '["ke","boo","la"]'],
                        'truncated' => false,
                    ],
                ],
                [
                    'array_int' => [
                        'value' => ['string_value' => '[4,5,6]'],
                        'truncated' => false,
                    ],
                    'array_string' => [
                        'value' => ['string_value' => '["ro","man"]'],
                        'truncated' => false,
                    ],
                    'array_struct_int_int' => [
                        'value' => ['string_value' => '[{"a":5,"b":6},{"a":7,"b":8}]'],
                        'truncated' => false,
                    ],
                    'array_struct_array_int' => [
                        'value' => ['string_value' => '[{"a":[5,6]},{"a":[7,8]}]'],
                        'truncated' => false,
                    ],
                    'array_struct_array-int_array-string' => [
                        'value' => ['string_value' => '[{"a":[5,6],"b":["e","f"]},{"a":[7,8],"b":["g","h"]}]'],
                        'truncated' => false,
                    ],
                    'array_very_long_string' => [
                        'value' => ['string_value' => '["ke","boo","la"]'],
                        'truncated' => false,
                    ],
                ],
                [
                    'array_int' => [
                        'value' => ['string_value' => '[7,8,9]'],
                        'truncated' => false,
                    ],
                    'array_string' => [
                        'value' => ['string_value' => '["789","456","123"]'],
                        'truncated' => false,
                    ],
                    'array_struct_int_int' => [
                        'value' => ['string_value' => '[{"a":9,"b":10},{"a":11,"b":12}]'],
                        'truncated' => false,
                    ],
                    'array_struct_array_int' => [
                        'value' => ['string_value' => '[{"a":[9,10]},{"a":[11,12]}]'],
                        'truncated' => false,
                    ],
                    'array_struct_array-int_array-string' => [
                        'value' => ['string_value' => '[{"a":[9,10],"b":["i","j"]},{"a":[11,12],"b":["k","l"]}]'],
                        'truncated' => false,
                    ],
                    'array_very_long_string' => [
                        //phpcs:ignore
                        'value' => ['string_value' => '["VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456Ve'],
                        'truncated' => true,
                    ],
                ],
                [
                    'array_int' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'array_string' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'array_struct_int_int' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'array_struct_array_int' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'array_struct_array-int_array-string' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'array_very_long_string' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                ],
            ],
        ];

        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);
    }

    public function testStructPreviewTable(): void
    {
        $tableName = $this->getTestHash() . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'id' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => false,
                ],
                'struct_int' => [
                    'type' => Bigquery::TYPE_STRUCT,
                    'length' => 'a INT64',
                    'nullable' => true,
                ],
                'struct_string' => [
                    'type' => Bigquery::TYPE_STRUCT,
                    'length' => 'a STRING',
                    'nullable' => true,
                ],
                'struct_int_int' => [
                    'type' => Bigquery::TYPE_STRUCT,
                    'length' => 'a INT64, b INT64',
                    'nullable' => true,
                ],
                'struct_struct_int_int' => [
                    'type' => Bigquery::TYPE_STRUCT,
                    'length' => 'x STRUCT<y INT64, z INT64>',
                    'nullable' => true,
                ],
                'struct_very_long_string' => [
                    'type' => Bigquery::TYPE_STRUCT,
                    'length' => 'a STRING',
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
                'columns' => '`id`, `struct_int`, `struct_string`, `struct_int_int`, `struct_struct_int_int`, `struct_very_long_string`',
                'rows' => [
                    //phpcs:ignore
                    "1, STRUCT(1), STRUCT('a'), STRUCT(1,2), STRUCT(STRUCT(1,2)), STRUCT('aaa')",
                    //phpcs:ignore
                    "2, STRUCT(3), STRUCT('b'), STRUCT(3,4), STRUCT(STRUCT(3,4)), STRUCT('bbb')",
                    //phpcs:ignore
                    sprintf("3, STRUCT(5), STRUCT('c'), STRUCT(5,6), STRUCT(STRUCT(5,6)), STRUCT('%s')", str_repeat('VeryLongString123456', 1000)),
                    '4, NULL, NULL, NULL, NULL, NULL',
                ],
            ],
        ];
        $this->fillTableWithData($this->projectCredentials, $bucketDatabaseName, $tableName, $insertGroups);

        // CHECK: all records + truncated
        $filter = [
            'input' => [
                //phpcs:ignore
                'columns' => [
                    'struct_int',
                    'struct_string',
                    'struct_int_int',
                    'struct_struct_int_int',
                    'struct_very_long_string',
                ],
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
                    ]),
                ],
            ],
            //phpcs:ignore
            'expectedColumns' => [
                'struct_int',
                'struct_string',
                'struct_int_int',
                'struct_struct_int_int',
                'struct_very_long_string',
            ],
            'expectedRows' => [
                [
                    'struct_int' => [
                        'value' => ['string_value' => '{"a":1}'],
                        'truncated' => false,
                    ],
                    'struct_string' => [
                        'value' => ['string_value' => '{"a":"a"}'],
                        'truncated' => false,
                    ],
                    'struct_int_int' => [
                        'value' => ['string_value' => '{"a":1,"b":2}'],
                        'truncated' => false,
                    ],
                    'struct_struct_int_int' => [
                        'value' => ['string_value' => '{"x":{"y":1,"z":2}}'],
                        'truncated' => false,
                    ],
                    'struct_very_long_string' => [
                        'value' => ['string_value' => '{"a":"aaa"}'],
                        'truncated' => false,
                    ],
                ],
                [
                    'struct_int' => [
                        'value' => ['string_value' => '{"a":3}'],
                        'truncated' => false,
                    ],
                    'struct_string' => [
                        'value' => ['string_value' => '{"a":"b"}'],
                        'truncated' => false,
                    ],
                    'struct_int_int' => [
                        'value' => ['string_value' => '{"a":3,"b":4}'],
                        'truncated' => false,
                    ],
                    'struct_struct_int_int' => [
                        'value' => ['string_value' => '{"x":{"y":3,"z":4}}'],
                        'truncated' => false,
                    ],
                    'struct_very_long_string' => [
                        'value' => ['string_value' => '{"a":"bbb"}'],
                        'truncated' => false,
                    ],
                ],
                [
                    'struct_int' => [
                        'value' => ['string_value' => '{"a":5}'],
                        'truncated' => false,
                    ],
                    'struct_string' => [
                        'value' => ['string_value' => '{"a":"c"}'],
                        'truncated' => false,
                    ],
                    'struct_int_int' => [
                        'value' => ['string_value' => '{"a":5,"b":6}'],
                        'truncated' => false,
                    ],
                    'struct_struct_int_int' => [
                        'value' => ['string_value' => '{"x":{"y":5,"z":6}}'],
                        'truncated' => false,
                    ],
                    'struct_very_long_string' => [
                        //phpcs:ignore
                        'value' => ['string_value' => '{"a":"VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString123456VeryLongString1234'],
                        'truncated' => true,
                    ],
                ],
                [
                    'struct_int' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'struct_string' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'struct_int_int' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'struct_struct_int_int' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'struct_very_long_string' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                ],
            ],
        ];

        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);
    }

    public function testPreviewTable(): void
    {
        $tableName = $this->getTestHash() . '_Test_table';
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
                'bytes' => [
                    'type' => Bigquery::TYPE_BYTES,
                    'nullable' => true,
                ],
                'geography' => [
                    'type' => Bigquery::TYPE_GEOGRAPHY,
                    'nullable' => true,
                ],
                'interval' => [
                    'type' => Bigquery::TYPE_INTERVAL,
                    'nullable' => true,
                ],
                'json' => [
                    'type' => Bigquery::TYPE_JSON,
                    'nullable' => true,
                ],
                'bigdecimal' => [
                    'type' => Bigquery::TYPE_BIGDECIMAL,
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
                'columns' => '`id`, `int`, `decimal`, `decimal_varchar`, `float`, `date`, `time`, `_timestamp`, `varchar`, `datetime`, `bytes`, `geography`, `interval`, `json`, `bigdecimal`',
                'rows' => [
                    //phpcs:ignore
                    "1, 100, 100.23, '100.23', 100.23456, '2022-01-01', '12:00:02', '2022-01-01 12:00:02', 'Variable character 1', '1989-08-31 00:00:00', b'\x01\x02\x03\x04', ST_GEOGPOINT(-122.4194, 37.7749), INTERVAL 1 YEAR, JSON'{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}', 1234567890.12345678901234567890",
                    // chanched `time` and `varchar`
                    //phpcs:ignore
                    "2, 100, 100.23, '100.20', 100.23456, '2022-01-01', '12:00:10', '2022-01-01 12:00:10', 'Variable 2', '1989-08-31 01:00:00.123456', b'\x01\x02\x03\x04', ST_GEOGPOINT(-122.4194, 37.7749), INTERVAL 1 YEAR, JSON'{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}', 1234567890.12345678901234567890",
                    sprintf(
                    //phpcs:ignore
                        "3, 200, 200.23, '200.23', 200.23456, '2022-01-02', '12:00:10', '2022-01-01 12:00:10', '%s', '1989-08-31 02:00:00', b'\x01\x02\x03\x04', ST_GEOGPOINT(-122.4194, 37.7749), INTERVAL 1 YEAR, JSON'{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}', 1234567890.12345678901234567890",
                        str_repeat('VeryLongString123456', 5),
                    ),
                    '4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
                ],
            ],
        ];
        $this->fillTableWithData($this->projectCredentials, $bucketDatabaseName, $tableName, $insertGroups);

        // CHECK: all records + truncated
        $filter = [
            'input' => [
                //phpcs:ignore
                'columns' => [
                    'id',
                    'int',
                    'decimal',
                    'float',
                    'date',
                    'time',
                    '_timestamp',
                    'varchar',
                    'datetime',
                    'bytes',
                    'geography',
                    'interval',
                    'json',
                    'bigdecimal',
                ],
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
                    ]),
                ],
            ],
            //phpcs:ignore
            'expectedColumns' => [
                'id',
                'int',
                'decimal',
                'float',
                'date',
                'time',
                '_timestamp',
                'varchar',
                'datetime',
                'bytes',
                'geography',
                'interval',
                'json',
                'bigdecimal',
            ],
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
                    'bytes' => [
                        'value' => ['string_value' => ''],
                        'truncated' => false,
                    ],
                    'geography' => [
                        'value' => ['string_value' => '{ "type": "Point", "coordinates": [-122.4194, 37.7749] } '],
                        'truncated' => false,
                    ],
                    'interval' => [
                        'value' => ['string_value' => '1-0 0 0:0:0'],
                        'truncated' => false,
                    ],
                    'json' => [
                        'value' => ['string_value' => '{"age":30,"city":"New York","name":"John"}'],
                        'truncated' => false,
                    ],
                    'bigdecimal' => [
                        'value' => ['string_value' => '1234567890.1234567890123456789'],
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
                    'bytes' => [
                        'value' => ['string_value' => ''],
                        'truncated' => false,
                    ],
                    'geography' => [
                        'value' => ['string_value' => '{ "type": "Point", "coordinates": [-122.4194, 37.7749] } '],
                        'truncated' => false,
                    ],
                    'interval' => [
                        'value' => ['string_value' => '1-0 0 0:0:0'],
                        'truncated' => false,
                    ],
                    'json' => [
                        'value' => ['string_value' => '{"age":30,"city":"New York","name":"John"}'],
                        'truncated' => false,
                    ],
                    'bigdecimal' => [
                        'value' => ['string_value' => '1234567890.1234567890123456789'],
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
                    'bytes' => [
                        'value' => ['string_value' => ''],
                        'truncated' => false,
                    ],
                    'geography' => [
                        'value' => ['string_value' => '{ "type": "Point", "coordinates": [-122.4194, 37.7749] } '],
                        'truncated' => false,
                    ],
                    'interval' => [
                        'value' => ['string_value' => '1-0 0 0:0:0'],
                        'truncated' => false,
                    ],
                    'json' => [
                        'value' => ['string_value' => '{"age":30,"city":"New York","name":"John"}'],
                        'truncated' => false,
                    ],
                    'bigdecimal' => [
                        'value' => ['string_value' => '1234567890.1234567890123456789'],
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
                    'bytes' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'geography' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'interval' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'json' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                    'bigdecimal' => [
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

        //phpcs:ignore
//        // CHECK: fulltext search @todo fulltext doesnt work for other types because is treated as string and fulltext search in all strings
//        $filter = [
//            'input' => [
//                'columns' => ['id', 'varchar'],
//                'filters' => new ExportFilters([
//                    'fulltextSearch' => 'character',
//                ]),
//            ],
//            'expectedColumns' => ['id', 'varchar'],
//            'expectedRows' => [
//                [
//                    'id' => [
//                        'value' => ['string_value' => '1'],
//                        'truncated' => false,
//                    ],
//                    'varchar' => [
//                        'value' => ['string_value' => 'Variable character 1'],
//                        'truncated' => false,
//                    ],
//                ],
//            ],
//        ];
//        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
//        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

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

        // CHECK: where filter withOUT datatype
        $filter = [
            'input' => [
                'columns' => ['id', 'decimal_varchar'],
                'filters' => new ExportFilters([
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'decimal_varchar',
                            'operator' => Operator::eq,
                            'values' => ['100.2'],
                            'dataType' => DataType::DOUBLE,
                            // here is datatype required, because the column is string
                        ]),
                        new TableWhereFilter([
                            'columnsName' => 'id',
                            'operator' => Operator::eq,
                            'values' => ['2'],
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
                        str_repeat('VeryLongString123456', 1000),
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
        $tableName = $this->getTestHash() . '_Test_table';
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
                $e->getMessage(),
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
                $e->getMessage(),
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
                $e->getMessage(),
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
                $e->getMessage(),
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

        try {
            $this->previewTable($bucketDatabaseName, $tableName, [
                'columns' => ['id', 'int', 'non-exist'],
            ]);
            $this->fail('This should never happen');
        } catch (ColumnNotFoundException $e) {
            $this->assertSame(2005, $e->getCode());
            $this->assertSame('Column "non-exist" not found in table definition.', $e->getMessage());
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
        $tableName = $this->getTestHash() . '_Test_table';
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
                'col_array' => [
                    'type' => Bigquery::TYPE_ARRAY,
                    'length' => 'INT64',
                    'nullable' => true,
                ],
                'col_struct' => [
                    'type' => Bigquery::TYPE_STRUCT,
                    'length' => 'a INT64, b INT64',
                    'nullable' => true,
                ],
                'col_bytes' => [
                    'type' => Bigquery::TYPE_BYTES,
                    'nullable' => true,
                ],
                'col_geography' => [
                    'type' => Bigquery::TYPE_GEOGRAPHY,
                    'nullable' => true,
                ],
                'col_interval' => [
                    'type' => Bigquery::TYPE_INTERVAL,
                    'nullable' => true,
                ],
                'col_json' => [
                    'type' => Bigquery::TYPE_JSON,
                    'nullable' => true,
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
        } catch (BadExportFilterParametersException $e) {
            $this->assertStringContainsString($expectExceptionMessage, $e->getMessage());
        }
    }

    public function filterProvider(): Generator
    {
        foreach ([
                     'ARRAY',
                     'STRUCT',
                     'BYTES',
                     'GEOGRAPHY',
                     'INTERVAL',
                     'JSON',
                 ] as $unfilterableType) {
            $columnName = 'col_' . strtolower($unfilterableType);
            yield $unfilterableType . ' is not filterable' => [
                [
                    'columns' => [$columnName],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => $columnName,
                                'operator' => Operator::eq,
                                'values' => ['aaa'],
                            ]),
                        ],
                    ]),
                ],
                sprintf(
                    'Filtering by column "%s" of type "%s" is not supported by the backend "bigquery".',
                    $columnName,
                    $unfilterableType,
                ),
            ];
        }
    }

    private function dropTable(string $databaseName, string $tableName): void
    {
        $handler = new DropTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $databaseName;
        $command = (new DropTableCommand())
            ->setPath($path)
            ->setTableName($tableName);

        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
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
        $handler->setInternalLogger($this->log);

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
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
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
                        $column->getColumnName(),
                    ));
                }

                // check column truncated
                $this->assertSame($expectedColumn['truncated'], $column->getIsTruncated());
            }
        }
    }

    public function testPartitionedTableWithRequirePartitionFilter(): void
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
            ->setNullable(false)
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = (new TableColumnShared)
            ->setName('time')
            ->setType(Bigquery::TYPE_TIMESTAMP)
            ->setNullable(false);
        $any = new Any();
        $any->pack(
            (new CreateTableCommand\BigQueryTableMeta())
                ->setClustering((new Clustering())->setFields(['id']))
                ->setRangePartitioning(
                    (new RangePartitioning())
                        ->setField('id')
                        ->setRange(
                            (new RangePartitioning\Range())
                                ->setStart('0')
                                ->setEnd('10')
                                ->setInterval('1'),
                        ),
                )
                ->setRequirePartitionFilter(true),
        );
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns)
            ->setMeta($any);
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        try {
            $this->previewTable(
                $bucketDatasetName,
                $tableName,
                [
                    'columns' => [
                        'id',
                        'time',
                    ],
                ],
            );
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            // 'Cannot query over table 'xxx.xxx' without a filter over column(s) 'id'
            // that can be used for partition elimination'
            $this->assertInstanceOf(BadExportFilterParametersException::class, $e);
            $this->assertStringContainsString(
                'without a filter over column(s) \'id\' that can be used for partition elimination',
                $e->getMessage(),
            );
        }
    }
}
