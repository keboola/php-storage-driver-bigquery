<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Preview\PreviewTableHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
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
        $this->createTable($bucketDatabaseName, $tableName, $tableStructure);

        // FILL DATA
        $insertGroups = [
            [
                'columns' => '`id`, `int`, `decimal`, `float`, `date`, `time`, `varchar`',
                'rows' => [
                    "1, 100, 100.23, 100.23456, '2022-01-01', '12:00:01', 'Variable character 1'",
                    sprintf(
                        "2, 200, 200.23, 200.23456, '2022-01-02', '12:00:02', '%s'",
                        str_repeat('VeryLongString123456', 5)
                    ),
                    '3, NULL, NULL, NULL, NULL, NULL, NULL',
                ],
            ],
        ];
        $this->fillTableWithData($bucketDatabaseName, $tableName, $insertGroups);

        // CHECK: all records + truncated
        $filter = [
            'input' => [
                'columns' => ['id', 'int', 'decimal', 'float', 'date', 'time', 'varchar'],
                'orderBy' => ['id' => PreviewTableCommand\PreviewTableOrderBy\Order::ASC],
            ],
            'expectedColumns' => ['id', 'int', 'decimal', 'float', 'date', 'time', 'varchar'],
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
                    'varchar' => [
                        'value' => ['string_value' => 'VeryLongString123456VeryLongString123456VeryLongSt'],
                        'truncated' => true,
                    ],
                ],
                [
                    'id' => [
                        'value' => ['string_value' => '3'],
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
                'columns' => ['id', 'int'],
                'orderBy' => ['int' => PreviewTableCommand\PreviewTableOrderBy\Order::DESC],
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
                        'value' => ['string_value' => '3'],
                        'truncated' => false,
                    ],
                    'int' => [
                        'value' => ['null_value' => NullValue::NULL_VALUE],
                        'truncated' => false,
                    ],
                ],
            ],
        ];
        $response = $this->previewTable($bucketDatabaseName, $tableName, $filter['input']);
        $this->checkPreviewData($response, $filter['expectedColumns'], $filter['expectedRows']);

        // CHECK: order by with dataType
        $filter = [
            'input' => [
                'columns' => ['id'],
                'orderBy' => ['date' => PreviewTableCommand\PreviewTableOrderBy\Order::ASC],
                'orderByDataType' => DataType::STRING,
            ],
            'expectedColumns' => ['id'],
            'expectedRows' => [
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
                        'value' => ['string_value' => '2'],
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
                'orderBy' => ['id' => PreviewTableCommand\PreviewTableOrderBy\Order::ASC],
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
                        'value' => ['string_value' => '200'],
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
        $this->createTable($bucketDatabaseName, $tableName, $tableStructure);

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

        // empty order by columnName
        try {
            $this->previewTable($bucketDatabaseName, $tableName, [
                'columns' => ['id', 'int'],
                'orderBy' => ['' => PreviewTableCommand\PreviewTableOrderBy\Order::ASC],
            ]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString('PreviewTableCommand.orderBy.columnName is required', $e->getMessage());
        }

        // wrong order by dataType
        try {
            $this->previewTable($bucketDatabaseName, $tableName, [
                'columns' => ['id', 'int'],
                'orderBy' => ['id' => PreviewTableCommand\PreviewTableOrderBy\Order::ASC],
                'orderByDataType' => DataType::DECIMAL,
            ]);
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            $this->assertStringContainsString(sprintf(
                'Data type %s not recognized. Possible datatypes are',
                DataType::DECIMAL
            ), $e->getMessage());
        }
    }

    /**
     * @param array{columns: array<string, array<string, mixed>>, primaryKeysNames: array<int, string>} $structure
     */
    private function createTable(string $databaseName, string $tableName, array $structure): void
    {
        $createTableHandler = new CreateTableHandler($this->clientManager);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $databaseName;

        $columns = new RepeatedField(GPBType::MESSAGE, CreateTableCommand\TableColumn::class);
        /** @var array{type: string, length: string, nullable: bool} $columnData */
        foreach ($structure['columns'] as $columnName => $columnData) {
            $columns[] = (new CreateTableCommand\TableColumn())
                ->setName($columnName)
                ->setType($columnData['type'])
                ->setLength($columnData['length'])
                ->setNullable($columnData['nullable']);
        }

//        $primaryKeysNames = new RepeatedField(GPBType::STRING);
//        foreach ($structure['primaryKeysNames'] as $primaryKeyName) {
//            $primaryKeysNames[] = $primaryKeyName;
//        }

        $createTableCommand = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns);
//            ->setPrimaryKeysNames($primaryKeysNames);

        $createTableResponse = $createTableHandler(
            $this->projectCredentials,
            $createTableCommand,
            []
        );
        $this->assertNull($createTableResponse);
    }

    /**
     * @param array{columns: string, rows: array<int, string>}[] $insertGroups
     */
    private function fillTableWithData(string $databaseName, string $tableName, array $insertGroups): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        foreach ($insertGroups as $insertGroup) {
            foreach ($insertGroup['rows'] as $insertRow) {
                $insertSql = sprintf(
                    "INSERT INTO %s.%s\n(%s) VALUES\n(%s);",
                    BigqueryQuote::quoteSingleIdentifier($databaseName),
                    BigqueryQuote::quoteSingleIdentifier($tableName),
                    $insertGroup['columns'],
                    $insertRow
                );
                $inserted = $bqClient->runQuery($bqClient->query($insertSql));
                $this->assertEquals(1, $inserted->info()['numDmlAffectedRows']);
            }
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
     * @param array{columns: array<string>, orderBy?: array<string, int>, orderByDataType?: int, limit?: int} $commandInput
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

        if (isset($commandInput['orderBy'])) {
            /** @var string $inputOrderByKey */
            $inputOrderByKey = key($commandInput['orderBy']);
            /** @var int $inputOrderByValue */
            $inputOrderByValue = current($commandInput['orderBy']);
            $orderBy = (new PreviewTableCommand\PreviewTableOrderBy())
                ->setColumnName($inputOrderByKey)
                ->setOrder($inputOrderByValue);
            if (isset($commandInput['orderByDataType'])) {
                $orderBy->setDataType($commandInput['orderByDataType']);
            }
            $command->setOrderBy($orderBy);
        }

        if (isset($commandInput['limit'])) {
            $command->setLimit($commandInput['limit']);
        }

        // TODO changeSince, changeUntil
        // TODO fulltextSearch
        // TODO whereFilters

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
