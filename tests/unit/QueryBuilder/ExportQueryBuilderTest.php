<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\QueryBuilder;

use Exception;
use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnNotFoundException;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ExportQueryBuilder;
use Keboola\StorageDriver\BigQuery\QueryBuilder\QueryBuilderException;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsCommand;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsCommand\WhereRefTableFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportFilters;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use PHPUnit\Framework\TestCase;

class ExportQueryBuilderTest extends TestCase
{
    /**
     * @dataProvider provideSuccessData
     * @param string[] $expectedBindings
     * @param ExportQueryBuilder::MODE_* $mode
     */
    public function testBuildQueryFromCommand(
        PreviewTableCommand $previewCommand,
        string $expectedSql,
        array $expectedBindings,
        int $rowsCount = 100_000,
        string $mode = ExportQueryBuilder::MODE_SELECT,
    ): void {
        $connection = $this->createMock(BigQueryClient::class);

        $columnConverter = new ColumnConverter();
        $tableColumnsDefinitions = new ColumnCollection($this->getColumnsCollection());

        // create query builder
        $qb = new ExportQueryBuilder($connection, $columnConverter);

        // build query
        $queryData = $qb->buildQueryFromCommand(
            $mode,
            $previewCommand->getFilters(),
            $previewCommand->getOrderBy(),
            $previewCommand->getColumns(),
            $tableColumnsDefinitions,
            'some_schema',
            'some_table',
            false,
            $rowsCount,
        );

        $this->assertSame(
            str_replace(PHP_EOL, '', $expectedSql),
            $queryData->getQuery(),
        );
        $this->assertSame(
            $expectedBindings,
            $queryData->getBindings(),
        );
    }

    public function provideSuccessData(): Generator
    {
        yield 'empty columns' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => [],
            ]),
            <<<SQL
            SELECT * FROM `some_schema`.`some_table`
            SQL,
            [],
        ];

        yield 'limit + one filter + orderBy' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 100,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => '',
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::ne,
                            'values' => ['foo'],
                            'dataType' => DataType::STRING,
                        ]),
                    ],
                ]),
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'name',
                        'order' => ExportOrderBy\Order::ASC,
                        'dataType' => DataType::STRING,
                    ]),
                ],
            ]),
            <<<SQL
            SELECT `some_table`.`id`, `some_table`.`name` FROM `some_schema`.`some_table` 
            WHERE `some_table`.`name` <> @dcValue1 
            ORDER BY `some_table`.`name` ASC LIMIT 100
            SQL,
            [
                'dcValue1' => 'foo',
            ],
        ];

        yield 'more filters + more orderBy' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => '',
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::ne,
                            'values' => ['foo'],
                            'dataType' => DataType::STRING,
                        ]),
                        new TableWhereFilter([
                            'columnsName' => 'height',
                            'operator' => Operator::ge,
                            'values' => ['1.23'],
                            'dataType' => DataType::STRING,
                        ]),
                    ],
                ]),
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
                        'dataType' => DataType::STRING,
                    ]),
                    new ExportOrderBy([
                        'columnName' => 'name',
                        'order' => ExportOrderBy\Order::DESC,
                        'dataType' => DataType::STRING,
                    ]),
                ],
            ]),
            <<<SQL
            SELECT `some_table`.`id`, `some_table`.`name` FROM `some_schema`.`some_table` 
            WHERE (`some_table`.`name` <> @dcValue1) 
            AND (`some_table`.`height` >= SAFE_CAST(@dcValue2 AS DECIMAL)) 
            ORDER BY `some_table`.`id` ASC, `some_table`.`name` DESC
            SQL,
            [
                'dcValue1' => 'foo',
                'dcValue2' => '1.23',
            ],
        ];

        yield 'search + more columns' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name', 'height', 'birth_at'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => 'foo',
                    'whereFilters' => [],
                ]),
                'orderBy' => [],
            ]),
            // @codingStandardsIgnoreStart
            <<<SQL
            SELECT `some_table`.`id`, `some_table`.`name`, `some_table`.`height`, `some_table`.`birth_at` FROM `some_schema`.`some_table` 
            WHERE `some_table`.`name` LIKE '%foo%'
            SQL,
            // @codingStandardsIgnoreEnd
            [],
        ];

        yield 'changeSince + changeUntil' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '1667293200',
                    'changeUntil' => '1669827600',

                    'fulltextSearch' => '',
                    'whereFilters' => [],
                ]),
                'orderBy' => [],
            ]),
            <<<SQL
            SELECT `some_table`.`id`, `some_table`.`name` FROM `some_schema`.`some_table` 
            WHERE (`some_table`.`_timestamp` >= @changedSince) AND (`some_table`.`_timestamp` < @changedUntil)
            SQL,
            [
                'changedSince' => '2022-11-01 09:00:00',
                'changedUntil' => '2022-11-30 17:00:00',
            ],
        ];

        yield 'changeSince' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '1667293200',
                    'fulltextSearch' => '',
                    'whereFilters' => [],
                ]),
                'orderBy' => [],
            ]),
            <<<SQL
            SELECT `some_table`.`id`, `some_table`.`name` FROM `some_schema`.`some_table` 
            WHERE `some_table`.`_timestamp` >= @changedSince
            SQL,
            [
                'changedSince' => '2022-11-01 09:00:00',
            ],
        ];

        yield 'changeUntil' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeUntil' => '1669827600',
                    'fulltextSearch' => '',
                    'whereFilters' => [],
                ]),
                'orderBy' => [],
            ]),
            <<<SQL
            SELECT `some_table`.`id`, `some_table`.`name` FROM `some_schema`.`some_table` 
            WHERE `some_table`.`_timestamp` < @changedUntil
            SQL,
            [
                'changedUntil' => '2022-11-30 17:00:00',
            ],
        ];

        yield 'one filter with type + orderBy with type' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name', 'height', 'birth_at'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => '',
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'height',
                            'operator' => Operator::ne,
                            'values' => ['10.20'],
                            'dataType' => DataType::REAL,
                        ]),
                    ],
                ]),
                'orderBy' => [
                    new ExportOrderBy([
                        'columnName' => 'id',
                        'order' => ExportOrderBy\Order::ASC,
                        'dataType' => DataType::REAL,
                    ]),
                ],
            ]),
            // @codingStandardsIgnoreStart
            <<<SQL
            SELECT `some_table`.`id`, `some_table`.`name`, `some_table`.`height`, `some_table`.`birth_at` FROM `some_schema`.`some_table` 
            WHERE `some_table`.`height` <> SAFE_CAST(@dcValue1 AS DECIMAL) 
            ORDER BY SAFE_CAST(`some_table`.`id` AS NUMERIC) ASC
            SQL,
            // @codingStandardsIgnoreEnd
            [
                'dcValue1' => '10.20',
            ],
        ];

        yield 'more filters with type' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'columns' => ['id', 'name', 'height', 'birth_at'],
                'tableName' => 'some_table',
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => '',
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'id',
                            'operator' => Operator::eq,
                            'values' => ['foo', 'bar'],
                            'dataType' => DataType::STRING,
                        ]),
                        new TableWhereFilter([
                            'columnsName' => 'id',
                            'operator' => Operator::ne,
                            'values' => ['50', '60'],
                            'dataType' => DataType::INTEGER,
                        ]),
                        new TableWhereFilter([
                            'columnsName' => 'height',
                            'operator' => Operator::ne,
                            'values' => ['10.20'],
                            'dataType' => DataType::REAL,
                        ]),
                    ],
                ]),
                'orderBy' => [],
            ]),
            // @codingStandardsIgnoreStart
            <<<SQL
            SELECT `some_table`.`id`, `some_table`.`name`, `some_table`.`height`, `some_table`.`birth_at` FROM `some_schema`.`some_table` 
            WHERE (`some_table`.`id` IN UNNEST([SAFE_CAST(@dcValue1 AS INT), SAFE_CAST(@dcValue2 AS INT)])) AND (`some_table`.`id` NOT IN UNNEST([SAFE_CAST(@dcValue3 AS INT), SAFE_CAST(@dcValue4 AS INT)])) 
            AND (`some_table`.`height` <> SAFE_CAST(@dcValue5 AS DECIMAL))
            SQL,
            // @codingStandardsIgnoreEnd
            [
                'dcValue1' => 'foo',
                'dcValue2' => 'bar',
                'dcValue3' => '50',
                'dcValue4' => '60',
                'dcValue5' => '10.20',
            ],
        ];

        yield 'preview 10_000 rows' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => [],
            ]),
            <<<SQL
            SELECT * FROM `some_schema`.`some_table`
            SQL,
            [],
            10_000,
            ExportQueryBuilder::MODE_PREVIEW,
        ];

        yield 'preview 10_001 rows' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => [],
            ]),
            <<<SQL
            SELECT * FROM `some_schema`.`some_table` TABLESAMPLE SYSTEM (10 PERCENT)
            SQL,
            [],
            10_001,
            ExportQueryBuilder::MODE_PREVIEW,
        ];

        yield 'preview 100_001 rows no limit' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => [],
                'filters' => new ExportFilters([]),
            ]),
            <<<SQL
            SELECT * FROM `some_schema`.`some_table` TABLESAMPLE SYSTEM (10 PERCENT)
            SQL,
            [],
            100_001,
            ExportQueryBuilder::MODE_PREVIEW,
        ];

        yield 'preview 100_001 rows with limit' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => [],
                'filters' => new ExportFilters([
                    'limit' => 100,
                ]),
            ]),
            <<<SQL
            SELECT * FROM `some_schema`.`some_table` TABLESAMPLE SYSTEM (1 PERCENT) LIMIT 100
            SQL,
            [],
            100_001,
            ExportQueryBuilder::MODE_PREVIEW,
        ];

        foreach ([
                     'changeSince' => [
                         '1667293200',
                         'SELECT * FROM `some_schema`.`some_table` WHERE `some_table`.`_timestamp` >= @changedSince',
                         [
                             'changedSince' => '2022-11-01 09:00:00',
                         ],
                     ],
                     'changeUntil' => [
                         '1669827600',
                         'SELECT * FROM `some_schema`.`some_table` WHERE `some_table`.`_timestamp` < @changedUntil',
                         [
                             'changedUntil' => '2022-11-30 17:00:00',
                         ],
                     ],
                     'fulltextSearch' => [
                         'xxx',
                         'SELECT * FROM `some_schema`.`some_table` WHERE `some_table`.`name` LIKE \'%xxx%\'',
                         [],
                     ],
                     'whereFilters' => [
                         [
                             new TableWhereFilter([
                                 'columnsName' => 'id',
                                 'operator' => Operator::eq,
                                 'values' => ['foo', 'bar'],
                                 'dataType' => DataType::STRING,
                             ]),
                         ],
                         // @phpcs:ignore
                         'SELECT * FROM `some_schema`.`some_table` WHERE `some_table`.`id` IN UNNEST([SAFE_CAST(@dcValue1 AS INT), SAFE_CAST(@dcValue2 AS INT)])',
                         [
                             'dcValue1' => 'foo',
                             'dcValue2' => 'bar',
                         ],
                     ],

                 ] as $name => [$value, $expectedSql, $bindings]) {
            yield 'preview 10_001 rows with filter' . $name => [
                new PreviewTableCommand([
                    'path' => ['some_schema'],
                    'tableName' => 'some_table',
                    'columns' => [],
                    'filters' => new ExportFilters([
                        $name => $value,
                    ]),
                ]),
                $expectedSql,
                $bindings,
                10_001,
                ExportQueryBuilder::MODE_PREVIEW,
            ];
        }
    }

    /**
     * @dataProvider provideFailedData
     * @param class-string<Exception> $exceptionClass
     */
    public function testBuildQueryFromCommandFailed(
        PreviewTableCommand $previewCommand,
        string $exceptionClass,
        string $exceptionMessage,
    ): void {
        $connection = $this->createMock(BigQueryClient::class);

        $columnConverter = new ColumnConverter();
        $tableColumnsDefinitions = new ColumnCollection($this->getColumnsCollection());

        // create query builder
        $qb = new ExportQueryBuilder($connection, $columnConverter);

        // build query
        $this->expectException($exceptionClass);
        $this->expectExceptionMessage($exceptionMessage);
        $qb->buildQueryFromCommand(
            ExportQueryBuilder::MODE_SELECT,
            $previewCommand->getFilters(),
            $previewCommand->getOrderBy(),
            $previewCommand->getColumns(),
            $tableColumnsDefinitions,
            'some_schema',
            '',
            true,
        );
    }

    public function provideFailedData(): Generator
    {
        yield 'select non exist column' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['non-existent', 'second-non-existent'],
            ]),
            ColumnNotFoundException::class,
            'Column "non-existent" not found in table definition.',
        ];

        yield 'fulltext + filter' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => 'word',
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::eq,
                            'values' => ['foo'],
                            'dataType' => DataType::STRING,
                        ]),
                    ],
                ]),
                'orderBy' => [],
            ]),
            QueryBuilderException::class,
            'Cannot use fulltextSearch and whereFilters at the same time',
        ];

        yield 'filter with multiple values and GT operator' => [
            new PreviewTableCommand([
                'path' => ['some_schema'],
                'tableName' => 'some_table',
                'columns' => ['id', 'name'],
                'filters' => new ExportFilters([
                    'limit' => 0,
                    'changeSince' => '',
                    'changeUntil' => '',

                    'fulltextSearch' => '',
                    'whereFilters' => [
                        new TableWhereFilter([
                            'columnsName' => 'name',
                            'operator' => Operator::gt,
                            'values' => ['foo', 'bar'],
                        ]),
                    ],
                ]),
                'orderBy' => [],
            ]),
            QueryBuilderException::class,
            'whereFilter with multiple values can be used only with "eq", "ne" operators',
        ];
    }

    /**
     * @dataProvider provideDeleteTableRowsData
     * @param string[] $expectedBindings
     */
    public function testBuildQueryFromDeleteTableRowsCommand(
        DeleteTableRowsCommand $command,
        string $expectedSql,
        array $expectedBindings,
    ): void {
        $connection = $this->createMock(BigQueryClient::class);
        $columnConverter = new ColumnConverter();

        $qb = new ExportQueryBuilder($connection, $columnConverter);
        $queryData = $qb->buildQueryFromCommand(
            ExportQueryBuilder::MODE_DELETE,
            (new ExportFilters())
                ->setChangeSince($command->getChangeSince())
                ->setChangeUntil($command->getChangeUntil())
                ->setWhereFilters($command->getWhereFilters()),
            new RepeatedField(GPBType::MESSAGE, ExportOrderBy::class),
            new RepeatedField(GPBType::STRING),
            new ColumnCollection($this->getColumnsCollection()),
            'some_bucket',
            $command->getTableName(),
            false,
            refTableFilters: $command->getWhereRefTableFilters(),
        );

        $this->assertSame(
            str_replace(PHP_EOL, '', $expectedSql),
            $queryData->getQuery(),
        );
        $this->assertSame(
            $expectedBindings,
            $queryData->getBindings(),
        );
    }

    public static function provideDeleteTableRowsData(): Generator
    {
        yield 'one whereRefTableFilters' => [
            new DeleteTableRowsCommand([
                'path' => ['some_bucket'],
                'tableName' => 'some_table',
                'whereRefTableFilters' => [
                    new WhereRefTableFilter([
                        'column' => 'id',
                        'operator' => Operator::eq,
                        'refColumn' => 'id',
                        'refPath' => ['universe','milkyway'],
                        'refTable' => 'mars',
                    ]),
                ],
            ]),
            'DELETE FROM `some_bucket`.`some_table` WHERE `some_table`.`id` IN (SELECT `id` FROM `universe.milkyway.mars`)', // phpcs:ignore
            [],
        ];

        yield 'array of whereRefTableFilters' => [
            new DeleteTableRowsCommand([
                'path' => ['some_bucket'],
                'tableName' => 'universe',
                'whereRefTableFilters' => [
                    new WhereRefTableFilter([
                        'column' => 'id',
                        'operator' => Operator::eq,
                        'refColumn' => 'id',
                        'refPath' => ['universe', 'milkyway'],
                        'refTable' => 'venus',
                    ]),
                    new WhereRefTableFilter([
                        'column' => 'name',
                        'operator' => Operator::ne,
                        'refColumn' => 'label',
                        'refPath' => ['farfaraway'],
                        'refTable' => 'alderaan',
                    ]),
                ],
            ]),
            'DELETE FROM `some_bucket`.`universe` WHERE (`universe`.`id` IN (SELECT `id` FROM `universe.milkyway.venus`)) AND (`universe`.`name` NOT IN (SELECT `label` FROM `farfaraway.alderaan`))', // phpcs:ignore
            [],
        ];

        yield 'combination of whereFilters and whereRefTableFilters' => [
            new DeleteTableRowsCommand([
                'path' => ['some_bucket'],
                'tableName' => 'universe',
                'whereFilters' => [
                    new TableWhereFilter([
                        'columnsName' => 'height',
                        'operator' => Operator::gt,
                        'values' => ['6378'],
                        'dataType' => DataType::INTEGER,
                    ]),
                ],
                'whereRefTableFilters' => [
                    new WhereRefTableFilter([
                        'column' => 'id',
                        'operator' => Operator::eq,
                        'refColumn' => 'id',
                        'refPath' => ['milkyway', 'sun'],
                        'refTable' => 'earth',
                    ]),
                ],
            ]),
            'DELETE FROM `some_bucket`.`universe` WHERE (`universe`.`height` > SAFE_CAST(@dcValue1 AS DECIMAL)) AND (`universe`.`id` IN (SELECT `id` FROM `milkyway.sun.earth`))', // phpcs:ignore
            [
                'dcValue1' => '6378',
            ],
        ];
    }

    public function testBuildQueryFromDeleteTableRowsCommandFailed(): void
    {
        $connection = $this->createMock(BigQueryClient::class);
        $columnConverter = new ColumnConverter();

        $command = new DeleteTableRowsCommand([
            'path' => ['some_bucket'],
            'tableName' => 'some_table',
            'whereRefTableFilters' => [
                new WhereRefTableFilter([
                    'column' => 'id',
                    'operator' => Operator::le,
                    'refColumn' => 'id',
                    'refPath' => ['wkspc'],
                    'refTable' => 'tmp_table',
                ]),
            ],
        ]);

        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage('Only IN or NOT INT operator is allowed.');

        $qb = new ExportQueryBuilder($connection, $columnConverter);
        $qb->buildQueryFromCommand(
            ExportQueryBuilder::MODE_DELETE,
            (new ExportFilters())
                ->setChangeSince($command->getChangeSince())
                ->setChangeUntil($command->getChangeUntil())
                ->setWhereFilters($command->getWhereFilters()),
            new RepeatedField(GPBType::MESSAGE, ExportOrderBy::class),
            new RepeatedField(GPBType::STRING),
            new ColumnCollection($this->getColumnsCollection()),
            'some_bucket',
            $command->getTableName(),
            false,
            refTableFilters: $command->getWhereRefTableFilters(),
        );
    }

    /** @return BigqueryColumn[] */
    public function getColumnsCollection(): array
    {
        // define table info
        $tableInfoColumns = [];
        $tableInfoColumns[] = new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_INT, [
            'length' => '',
            'nullable' => false,
            'default' => '',
        ]));
        $tableInfoColumns[] = new BigqueryColumn('name', new Bigquery(Bigquery::TYPE_STRING, [
            'length' => '100',
            'nullable' => true,
            'default' => '',
        ]));
        $tableInfoColumns[] = new BigqueryColumn('height', new Bigquery(Bigquery::TYPE_DECIMAL, [
            'length' => '4,2',
            'nullable' => true,
            'default' => '',
        ]));
        $tableInfoColumns[] = new BigqueryColumn('birth_at', new Bigquery(Bigquery::TYPE_DATE, [
            'length' => '',
            'nullable' => true,
            'default' => '',
        ]));
        return $tableInfoColumns;
    }
}
