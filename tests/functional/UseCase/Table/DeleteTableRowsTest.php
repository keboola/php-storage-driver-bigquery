<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\DeleteTableRowsHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Preview\PreviewTableHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsCommand;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsResponse;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;

class DeleteTableRowsTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    private function setData(string $bucketDatabaseName, string $tableName): void
    {
// FILL DATA
        $insertGroups = [
            [
                //phpcs:ignore
                'columns' => '`id`, `int`, `decimal`,`decimal_varchar`, `float`, `date`, `time`, `_timestamp`, `varchar`',
                'rows' => [
                    //phpcs:ignore
                    "1, 100, 100.23, '100.23', 100.23456, '2022-01-01', '12:00:02', '2022-01-01 12:00:02', 'Variable character 1'",
                    // chanched `time` and `varchar`
                    //phpcs:ignore
                    "2, 100, 100.23, '100.20', 100.23456, '2022-01-01', '12:00:10', '2022-01-01 12:00:10', 'Variable 2'",
                    sprintf(
                        "3, 200, 200.23, '200.23', 200.23456, '2022-01-02', '12:00:10', '2022-01-01 12:00:10', '%s'",
                        str_repeat('VeryLongString123456', 5)
                    ),
                    '4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
                ],
            ],
        ];
        $this->fillTableWithData(
            $this->projectCredentials,
            $bucketDatabaseName,
            $tableName,
            $insertGroups,
            true
        );
    }

    /**
     * @param DeleteTableRowsCommand|PreviewTableCommand $command
     */
    private function setPath(string $databaseName, Message $command, string $tableName): void
    {
        if ($databaseName) {
            $path = new RepeatedField(GPBType::STRING);
            $path[] = $databaseName;
            $command->setPath($path);
        }

        if ($tableName) {
            $command->setTableName($tableName);
        }
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];

        $this->bucketResponse = $this->createTestBucket($this->projects[0][0], $this->projects[0][2]);
    }

    public function testDeleteTableRows(): void
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
            ],
            'primaryKeysNames' => ['id'],
        ];
        $this->createTable($this->projectCredentials, $bucketDatabaseName, $tableName, $tableStructure);
        $this->setData($bucketDatabaseName, $tableName);

        // CHECK: no filter = truncate table
        $filter = [
            'input' => [],
            'expectedRows' => [],
        ];
        $response = $this->deleteRows($bucketDatabaseName, $tableName, $filter['input'], 4, 0);
        $this->checkPreviewData($response, $filter['expectedRows']);

        // CHECK: changeSince + changeUntil
        $filter = [
            'input' => [
                'changeSince' => '1641038401',
                'changeUntil' => '1641038403',
            ],
            'expectedRows' => ['2', '3', '4'],
        ];
        $this->setData($bucketDatabaseName, $tableName);
        $response = $this->deleteRows($bucketDatabaseName, $tableName, $filter['input'], 1, 3);
        $this->checkPreviewData($response, $filter['expectedRows']);

        // CHECK: simple where filter
        $filter = [
            'input' => [
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'int',
                        'operator' => Operator::ge,
                        'values' => ['100'],
                        'dataType' => DataType::INTEGER,
                    ]),
                ],
            ],
            'expectedRows' => ['4'],
        ];
        $this->setData($bucketDatabaseName, $tableName);
        $response = $this->deleteRows($bucketDatabaseName, $tableName, $filter['input'], 3, 1);
        $this->checkPreviewData($response, $filter['expectedRows']);

        // CHECK: multiple where filters
        $filter = [
            'input' => [
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
            ],
            'expectedRows' => ['1', '2', '4'],
        ];
        $this->setData($bucketDatabaseName, $tableName);
        $response = $this->deleteRows($bucketDatabaseName, $tableName, $filter['input'], 1, 3);
        $this->checkPreviewData($response, $filter['expectedRows']);

        // CHECK: where filter with datatype
        $filter = [
            'input' => [
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'decimal_varchar',
                        'operator' => Operator::eq,
                        'values' => ['100.2'],
                        'dataType' => DataType::REAL,
                    ]),
                ],
            ],
            'expectedRows' => ['1', '3', '4'],
        ];
        $this->setData($bucketDatabaseName, $tableName);
        $response = $this->deleteRows($bucketDatabaseName, $tableName, $filter['input'], 1, 3);
        $this->checkPreviewData($response, $filter['expectedRows']);

        // DROP TABLE
        $this->dropTable($bucketDatabaseName, $tableName);
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
     *     changeUntil?: string,
     *     changeSince?: string,
     *     whereFilters?: TableWhereFilter[]
     * } $commandInput
     */
    private function deleteRows(
        string $databaseName,
        string $tableName,
        array $commandInput,
        int $expectedDeletedRowsCount,
        int $expectedRowsCount
    ): PreviewTableResponse {
        $handler = new DeleteTableRowsHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new DeleteTableRowsCommand();
        $this->setPath($databaseName, $command, $tableName);

        if (array_key_exists('changeUntil', $commandInput)) {
            $command->setChangeUntil($commandInput['changeUntil']);
        }
        if (array_key_exists('changeSince', $commandInput)) {
            $command->setChangeSince($commandInput['changeSince']);
        }
        if (array_key_exists('whereFilters', $commandInput)) {
            $command->setWhereFilters($commandInput['whereFilters']);
        }
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(DeleteTableRowsResponse::class, $response);
        $this->assertSame($expectedDeletedRowsCount, $response->getDeletedRowsCount());
        $this->assertSame($expectedRowsCount, $response->getTableRowsCount());

        // preview data
        $handler = new PreviewTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new PreviewTableCommand();

        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'id';
        $command->setColumns($columns);
        $this->setPath($databaseName, $command, $tableName);

        $orderBy = new RepeatedField(GPBType::MESSAGE, ExportOrderBy::class);
        $orderBy[] =
            new ExportOrderBy([
                'columnName' => 'id',
                'order' => ExportOrderBy\Order::ASC,
            ]);
        $command->setOrderBy($orderBy);

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
     * @param string[] $expectedRows
     */
    private function checkPreviewData(PreviewTableResponse $response, array $expectedRows): void
    {
        // check rows
        $this->assertCount(count($expectedRows), $response->getRows());
        /** @var PreviewTableResponse\Row[] $rows */
        $rows = $response->getRows();
        foreach ($rows as $rowNumber => $row) {
            $expectedId = $expectedRows[$rowNumber];
            /** @var PreviewTableResponse\Row\Column[] $columns */
            $columns = $row->getColumns();
            foreach ($columns as $column) {
                // check column value
                /** @var Value $columnValue */
                $columnValue = $column->getValue();
                $this->assertSame($expectedId, $columnValue->getStringValue());
            }
        }
    }
}
