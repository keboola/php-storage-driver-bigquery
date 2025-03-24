<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\DeleteRows;

use Generator;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\DeleteTableRowsHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Preview\PreviewTableHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsCommand;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsCommand\WhereRefTableFilter;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy\Order;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class DeleteByTableInWorkspaceTest extends BaseCase
{
    private const BUCKET_TABLE_STRUCTURE = [
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

    private const BUCKET_TABLE_DATA = [
        // phpcs:disable
        // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
        "1, 101, 100.11, '100.11', 100.23456, '2001-01-10', '11:01:10', '2001-01-10 11:01:10', 'First of all'",
        "2, 22, 200.22, '200.22', 200.23456, '2022-02-20', '2:22:02', '2022-02-22 2:22:02', 'Second row'",
        "3, 303, 300.33, '300.33', 300.23456, '2003-03-13', '3:13:33', '2003-03-13 3:13:33', 'TristaTricetTriStribrnychStrikacekStrikaloPresTristaTricetTriStribrnychStrech.Dyjadyjada.Dyjadyjada.'",
        "10, 101, 10101.01, '1010.01', 1010.012345, '2010-11-12', '11:01:10', '2010-11-12 10:11:12', 'Number ten'",
        "20, 22, 2020.22, '2020.22', 2020.012345, '2022-02-20', '20:22:02', '2020-02-20 20:22:02', 'Twenty bucks'",
        '42, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
        // phpcs:enable
    ];

    private Project $project;

    private Table $bucketTable;

    /**
     * @dataProvider oneTableProvider
     * @param string[] $workspaceTableRows
     * @param array<array{
     *     column: string,
     *     operator: string,
     *     refColumn: string,
     * }> $filters
     * @param int[] $remainingBucketTableRowIds
     */
    public function testOneTable(array $workspaceTableRows, array $filters, array $remainingBucketTableRowIds): void
    {
        $workspace = $this->createWorkspaceInProject($this->project);
        $workspaceTable = $this->createTableInDataset($workspace, 'delete_by_table', self::BUCKET_TABLE_STRUCTURE);
        $this->insertIntoTable($workspaceTable, $workspaceTableRows);

        $handler = new DeleteTableRowsHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $whereRefTableFilters = [];
        foreach ($filters as $filter) {
            $whereRefTableFilters[] = new DeleteTableRowsCommand\WhereRefTableFilter(
                $filter + [
                    'refPath' => [$workspaceTable->dataset->name],
                    'refTable' => $workspaceTable->name,
                ],
            );
        }

        $command = new DeleteTableRowsCommand([
            'path' => [$this->bucketTable->dataset->name],
            'tableName' => $this->bucketTable->name,
            'whereRefTableFilters' => $whereRefTableFilters,
        ]);

        $response = $handler(
            $this->project->credentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(DeleteTableRowsResponse::class, $response);
        $this->assertBucketTableContainsRows($remainingBucketTableRowIds);
    }

    public static function oneTableProvider(): Generator
    {
        yield 'id' => [
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                '1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
            ],
            [
                [
                    'column' => 'id', 'operator' => Operator::eq, 'refColumn' => 'id',
                ],
            ],
            [2, 3, 10, 20, 42],
        ];
        yield 'int' => [
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                '71, 101, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
            ],
            [
                [
                    'column' => 'int', 'operator' => Operator::eq, 'refColumn' => 'int',
                ],
            ],
            [2, 3, 20, 42],
        ];
        yield 'decimal' => [
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                '72, NULL, 300.33, NULL, NULL, NULL, NULL, NULL, NULL',
            ],
            [
                [
                    'column' => 'decimal', 'operator' => Operator::eq, 'refColumn' => 'decimal',
                ],
            ],
            [1, 2, 10, 20, 42],
        ];
        yield 'decimal_varchar' => [
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                "73, NULL, NULL, '300.33', NULL, NULL, NULL, NULL, NULL",
            ],
            [
                [
                    'column' => 'decimal_varchar', 'operator' => Operator::eq, 'refColumn' => 'decimal_varchar',
                ],
            ],
            [1, 2, 10, 20, 42],
        ];
        yield 'float' => [
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                '74, NULL, NULL, NULL, 200.23456, NULL, NULL, NULL, NULL',
            ],
            [
                [
                    'column' => 'float', 'operator' => Operator::eq, 'refColumn' => 'float',
                ],
            ],
            [1, 3, 10, 20, 42],
        ];
        yield 'date' => [
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                "75, NULL, NULL, NULL, NULL, '2022-02-20', NULL, NULL, NULL",
            ],
            [
                [
                    'column' => 'date', 'operator' => Operator::eq, 'refColumn' => 'date',
                ],
            ],
            [1, 3, 10, 42],
        ];
        yield 'time' => [
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                "76, NULL, NULL, NULL, NULL, NULL, '11:01:10', NULL, NULL",
            ],
            [
                [
                    'column' => 'time', 'operator' => Operator::eq, 'refColumn' => 'time',
                ],
            ],
            [2, 3, 20, 42],
        ];
        yield '_timestamp' => [
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                "77, NULL, NULL, NULL, NULL, NULL, NULL, '2003-03-13 3:13:33', NULL",
            ],
            [
                [
                    'column' => '_timestamp', 'operator' => Operator::eq, 'refColumn' => '_timestamp',
                ],
            ],
            [1, 2, 10, 20, 42],
        ];
        yield 'varchar' => [
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                "78, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Twenty bucks'",
            ],
            [
                [
                    'column' => 'varchar', 'operator' => Operator::eq, 'refColumn' => 'varchar',
                ],
            ],
            [1, 2, 3, 10, 42],
        ];
    }

    public function testTwoTables(): void
    {
        $workspace = $this->createWorkspaceInProject($this->project);

        $workspaceTableT1 = $this->createTableInDataset($workspace, 'delete_by_table_t1', self::BUCKET_TABLE_STRUCTURE);
        $this->insertIntoTable(
            $workspaceTableT1,
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                '2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
            ],
        );

        $workspaceTableT2 = $this->createTableInDataset($workspace, 'delete_by_table_t2', self::BUCKET_TABLE_STRUCTURE);
        $this->insertIntoTable(
            $workspaceTableT2,
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                '1, 22, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
            ],
        );

        $whereRefTableFilters = [
            new WhereRefTableFilter([
                'column' => 'id',
                'operator' => Operator::eq,
                'refColumn' => 'id',
                'refPath' => [$workspaceTableT1->dataset->name],
                'refTable' => $workspaceTableT1->name,
            ]),
            new WhereRefTableFilter([
                'column' => 'int',
                'operator' => Operator::eq,
                'refColumn' => 'int',
                'refPath' => [$workspaceTableT2->dataset->name],
                'refTable' => $workspaceTableT2->name,
            ]),
        ];

        $handler = new DeleteTableRowsHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = new DeleteTableRowsCommand([
            'path' => [$this->bucketTable->dataset->name],
            'tableName' => $this->bucketTable->name,
            'whereRefTableFilters' => $whereRefTableFilters,
        ]);

        $response = $handler(
            $this->project->credentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(DeleteTableRowsResponse::class, $response);
        $this->assertBucketTableContainsRows([1, 3, 10, 20, 42]);
    }

    public function testWithOtherConditions(): void
    {
        $workspace = $this->createWorkspaceInProject($this->project);

        $workspaceTable = $this->createTableInDataset($workspace, 'delete_by_table', self::BUCKET_TABLE_STRUCTURE);
        $this->insertIntoTable(
            $workspaceTable,
            [
                // id, int, decimal, decimal_varchar, float, date, time, _timestamp, varchar
                '1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
                '2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
                '3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL',
            ],
        );

        $whereFilters = [
            new TableWhereFilter([
                'columnsName' => 'int',
                'operator' => Operator::le,
                'values' => ['300'],
                'dataType' => DataType::INTEGER,
            ]),
        ];

        $whereRefTableFilters = [
            new WhereRefTableFilter([
                'column' => 'id',
                'operator' => Operator::eq,
                'refColumn' => 'id',
                'refPath' => [$workspaceTable->dataset->name],
                'refTable' => $workspaceTable->name,
            ]),
        ];

        $command = new DeleteTableRowsCommand([
            'path' => [$this->bucketTable->dataset->name],
            'tableName' => $this->bucketTable->name,
            'whereFilters' => $whereFilters,
            'whereRefTableFilters' => $whereRefTableFilters,
        ]);

        $handler = new DeleteTableRowsHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $response = $handler(
            $this->project->credentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(DeleteTableRowsResponse::class, $response);
        $this->assertBucketTableContainsRows([3, 10, 20, 42]);
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->project = new Project($this->projects[0][0], $this->projects[0][1]);

        $bucket = $this->createBucketInProject($this->project);
        $table = $this->createTableInDataset($bucket, 'storage_data', self::BUCKET_TABLE_STRUCTURE);
        $this->insertIntoTable($table, self::BUCKET_TABLE_DATA);

        $this->bucketTable = $table;
    }

    private function createBucketInProject(Project $project): Bucket
    {
        $bucketResponse = $this->createTestBucket($project->credentials);

        return new Bucket($project->credentials, $bucketResponse);
    }

    private function createWorkspaceInProject(Project $project): Workspace
    {
        [$credentials, $response] = $this->createTestWorkspace($project->credentials, $project->response);

        return new Workspace($credentials, $response);
    }

    /**
     * @param array{
     *     columns: array<string, array<string, mixed>>,
     *     primaryKeysNames?: array<int, string>
     * } $structure
     */
    private function createTableInDataset(
        Bucket|Workspace $dataset,
        string $name,
        array $structure,
    ): Table {
        $this->createTable($dataset->credentials, $dataset->name, $name, $structure);

        return new Table($dataset, $name, $structure);
    }

    /**
     * @param list<string> $rows
     */
    private function insertIntoTable(
        Table $table,
        array $rows,
    ): void {
        $columns = implode(
            ', ',
            array_map(fn($col) => BigqueryQuote::quoteSingleIdentifier($col), $table->columns),
        );

        $this->fillTableWithData(
            $table->dataset->credentials,
            $table->dataset->name,
            $table->name,
            [
                [
                    'columns' => $columns,
                    'rows' => $rows,
                ],
            ],
        );
    }

    /**
     * @param int[] $rowIds
     */
    private function assertBucketTableContainsRows(array $rowIds): void
    {
        $handler = new PreviewTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = new PreviewTableCommand([
            'path' => [$this->bucketTable->dataset->name],
            'tableName' => $this->bucketTable->name,
            'columns' => ['id'],
            'orderBy' => [
                new ExportOrderBy([
                    'columnName' => 'id',
                    'order' => Order::ASC,
                ]),
            ],
        ]);

        $response = $handler(
            $this->project->credentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(PreviewTableResponse::class, $response);

        $this->assertCount(count($rowIds), $response->getRows());

        /** @var PreviewTableResponse\Row[] $rows */
        $rows = $response->getRows();

        foreach ($rows as $rowKey => $row) {
            /** @var PreviewTableResponse\Row\Column[] $columns */
            $columns = $row->getColumns();

            foreach ($columns as $column) {
                /** @var Value $columnValue */
                $columnValue = $column->getValue();
                $this->assertSame((string) $rowIds[$rowKey], $columnValue->getStringValue());
            }
        }
    }
}
