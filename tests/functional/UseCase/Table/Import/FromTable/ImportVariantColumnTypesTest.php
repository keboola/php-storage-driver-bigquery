<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\FromTable;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

/**
 * Tests import functionality with various column type and nullability combinations.
 *
 * IMPORTANT: BigQuery VIEW Limitation
 * ------------------------------------
 * BigQuery logical views do NOT preserve NOT NULL constraints from source tables.
 * This is expected behavior because:
 *
 * 1. Views are virtual tables (stored queries, not physical data)
 * 2. Views are read-only - no INSERT/UPDATE/DELETE operations
 * 3. Constraint enforcement happens at the table level during data modifications
 * 4. View query results could potentially include NULLs depending on query logic
 *    (e.g., LEFT JOINs, CASE statements, COALESCE functions)
 *
 * When you create a view with "CREATE VIEW AS SELECT * FROM table", BigQuery:
 * - Preserves column types (INTEGER, STRING, etc.)
 * - Does NOT preserve NOT NULL constraints
 * - Shows all columns as nullable in the view's schema metadata
 *
 * This test validates both scenarios:
 * - FULL imports: Creates physical tables, preserves NOT NULL constraints
 * - VIEW imports: Creates logical views, all columns become nullable
 *
 * Reference: https://cloud.google.com/bigquery/docs/views-intro
 *
 * @group Import
 */
class ImportVariantColumnTypesTest extends BaseImportTestCase
{
    /**
     * phpcs:disable
     * @return Generator<string, array{array<array{name: string, type: string, nullable: bool}>, array<array<string|int|float|null>>, int}>
     * phpcs:enable
     */
    public function columnConfigAndImportTypeProvider(): Generator
    {
        // All nullable columns with VIEW import
        yield 'all nullable with VIEW' => [
            [
                ['name' => 'id', 'type' => Bigquery::TYPE_INT, 'nullable' => true],
                ['name' => 'name', 'type' => Bigquery::TYPE_STRING, 'nullable' => true],
                ['name' => 'price', 'type' => Bigquery::TYPE_FLOAT64, 'nullable' => true],
            ],
            [
                [1, 'Product A', 10.5],
                [2, 'Product B', 20.0],
                [null, null, null],
            ],
            ImportOptions\ImportType::VIEW,
        ];

        // All nullable columns with FULL import
        yield 'all nullable with FULL' => [
            [
                ['name' => 'id', 'type' => Bigquery::TYPE_INT, 'nullable' => true],
                ['name' => 'name', 'type' => Bigquery::TYPE_STRING, 'nullable' => true],
                ['name' => 'price', 'type' => Bigquery::TYPE_FLOAT64, 'nullable' => true],
            ],
            [
                [1, 'Product A', 10.5],
                [2, 'Product B', 20.0],
                [null, null, null],
            ],
            ImportOptions\ImportType::FULL,
        ];

        // All not nullable with VIEW import
        yield 'all not nullable with VIEW' => [
            [
                ['name' => 'id', 'type' => Bigquery::TYPE_INT, 'nullable' => false],
                ['name' => 'name', 'type' => Bigquery::TYPE_STRING, 'nullable' => false],
                ['name' => 'amount', 'type' => Bigquery::TYPE_NUMERIC, 'nullable' => false],
            ],
            [
                [1, 'Item A', 100],
                [2, 'Item B', 200],
            ],
            ImportOptions\ImportType::VIEW,
        ];

        // All not nullable with FULL import
        yield 'all not nullable with FULL' => [
            [
                ['name' => 'id', 'type' => Bigquery::TYPE_INT, 'nullable' => false],
                ['name' => 'name', 'type' => Bigquery::TYPE_STRING, 'nullable' => false],
                ['name' => 'amount', 'type' => Bigquery::TYPE_NUMERIC, 'nullable' => false],
            ],
            [
                [1, 'Item A', 100],
                [2, 'Item B', 200],
            ],
            ImportOptions\ImportType::FULL,
        ];

        // Mixed nullable and not nullable with VIEW import
        yield 'mixed nullable with VIEW' => [
            [
                ['name' => 'id', 'type' => Bigquery::TYPE_INT64, 'nullable' => false],
                ['name' => 'category', 'type' => Bigquery::TYPE_STRING, 'nullable' => true],
                ['name' => 'quantity', 'type' => Bigquery::TYPE_INT, 'nullable' => true],
                ['name' => 'status', 'type' => Bigquery::TYPE_STRING, 'nullable' => false],
            ],
            [
                [1, 'Electronics', 50, 'active'],
                [2, null, null, 'inactive'],
                [3, 'Books', 25, 'active'],
            ],
            ImportOptions\ImportType::VIEW,
        ];

        // Mixed nullable and not nullable with FULL import
        yield 'mixed nullable with FULL' => [
            [
                ['name' => 'id', 'type' => Bigquery::TYPE_INT64, 'nullable' => false],
                ['name' => 'category', 'type' => Bigquery::TYPE_STRING, 'nullable' => true],
                ['name' => 'quantity', 'type' => Bigquery::TYPE_INT, 'nullable' => true],
                ['name' => 'status', 'type' => Bigquery::TYPE_STRING, 'nullable' => false],
            ],
            [
                [1, 'Electronics', 50, 'active'],
                [2, null, null, 'inactive'],
                [3, 'Books', 25, 'active'],
            ],
            ImportOptions\ImportType::FULL,
        ];

        // Complex mixed types with VIEW import
        yield 'complex mixed types with VIEW' => [
            [
                ['name' => 'col_int', 'type' => Bigquery::TYPE_INT, 'nullable' => false],
                ['name' => 'col_bigint', 'type' => Bigquery::TYPE_BIGINT, 'nullable' => true],
                ['name' => 'col_float', 'type' => Bigquery::TYPE_FLOAT64, 'nullable' => true],
                ['name' => 'col_numeric', 'type' => Bigquery::TYPE_NUMERIC, 'nullable' => false],
                ['name' => 'col_string', 'type' => Bigquery::TYPE_STRING, 'nullable' => true],
            ],
            [
                [1, 9223372036854775807, 3.14, 99.99, 'test1'],
                [2, null, null, 199.99, null],
                [3, 123456789, 2.71, 299.99, 'test3'],
            ],
            ImportOptions\ImportType::VIEW,
        ];

        // Complex mixed types with FULL import
        yield 'complex mixed types with FULL' => [
            [
                ['name' => 'col_int', 'type' => Bigquery::TYPE_INT, 'nullable' => false],
                ['name' => 'col_bigint', 'type' => Bigquery::TYPE_BIGINT, 'nullable' => true],
                ['name' => 'col_float', 'type' => Bigquery::TYPE_FLOAT64, 'nullable' => true],
                ['name' => 'col_numeric', 'type' => Bigquery::TYPE_NUMERIC, 'nullable' => false],
                ['name' => 'col_string', 'type' => Bigquery::TYPE_STRING, 'nullable' => true],
            ],
            [
                [1, 9223372036854775807, 3.14, 99.99, 'test1'],
                [2, null, null, 199.99, null],
                [3, 123456789, 2.71, 299.99, 'test3'],
            ],
            ImportOptions\ImportType::FULL,
        ];

        // Edge case with VIEW import
        yield 'edge case with VIEW' => [
            [
                ['name' => 'pk', 'type' => Bigquery::TYPE_INT, 'nullable' => false],
                ['name' => 'opt1', 'type' => Bigquery::TYPE_STRING, 'nullable' => true],
                ['name' => 'opt2', 'type' => Bigquery::TYPE_INT, 'nullable' => true],
                ['name' => 'opt3', 'type' => Bigquery::TYPE_FLOAT64, 'nullable' => true],
            ],
            [
                [1, null, null, null],
                [2, 'value', null, null],
                [3, 'value', 42, 3.14],
            ],
            ImportOptions\ImportType::VIEW,
        ];

        // Edge case with FULL import
        yield 'edge case with FULL' => [
            [
                ['name' => 'pk', 'type' => Bigquery::TYPE_INT, 'nullable' => false],
                ['name' => 'opt1', 'type' => Bigquery::TYPE_STRING, 'nullable' => true],
                ['name' => 'opt2', 'type' => Bigquery::TYPE_INT, 'nullable' => true],
                ['name' => 'opt3', 'type' => Bigquery::TYPE_FLOAT64, 'nullable' => true],
            ],
            [
                [1, null, null, null],
                [2, 'value', null, null],
                [3, 'value', 42, 3.14],
            ],
            ImportOptions\ImportType::FULL,
        ];
    }

    /**
     * Test import with various column types and nullability configurations.
     *
     * This test validates that the import handler correctly:
     * 1. Preserves column data types during import (INTEGER, STRING, FLOAT64, etc.)
     * 2. Handles nullable and NOT NULL columns appropriately
     * 3. Imports data correctly for both VIEW and FULL import types
     *
     * IMPORTANT: For VIEW imports, all columns will be nullable regardless of
     * the source table's NOT NULL constraints. This is a BigQuery limitation
     * documented in the class-level docblock.
     *
     * @dataProvider columnConfigAndImportTypeProvider
     * @param array<array{name: string, type: string, nullable: bool}> $columns
     * @param array<array<string|null>> $testData
     */
    public function testImportWithVariantColumnTypes(
        array $columns,
        array $testData,
        int $importType,
    ): void {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_src';
        $destinationTableName = $this->getTestHash() . '_dest';

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // Create source table with specified columns
        $this->createSourceTableWithColumns(
            $bucketDatabaseName,
            $sourceTableName,
            $columns,
            $bqClient,
        );

        // Insert test data
        $this->insertTestData(
            $bucketDatabaseName,
            $sourceTableName,
            $columns,
            $testData,
            $bqClient,
        );

        // Create destination table with same columns (only for FULL import, not VIEW)
        if ($importType !== ImportOptions\ImportType::VIEW) {
            $this->createSourceTableWithColumns(
                $bucketDatabaseName,
                $destinationTableName,
                $columns,
                $bqClient,
            );
        }

        // Create import command
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        // Set up column mappings (1:1 mapping for all columns)
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach ($columns as $column) {
            $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($column['name'])
                ->setDestinationColumnName($column['name']);
        }

        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
                ->setColumnMappings($columnMappings),
        );

        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName),
        );

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType($importType)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE), // Required for typed tables!
        );

        // Execute import
        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify response
        $this->assertInstanceOf(TableImportResponse::class, $response);

        $expectedRowCount = count($testData);
        $isView = ($importType === ImportOptions\ImportType::VIEW);

        if ($isView) {
            // VIEW import returns 0 from getImportedRowsCount()
            $this->assertSame(0, $response->getImportedRowsCount());
        } else {
            // FULL import returns actual row count
            $this->assertSame($expectedRowCount, $response->getImportedRowsCount());
        }

        // Verify data integrity using direct query (works for both VIEW and TABLE)
        $actualData = $this->fetchTable($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertCount($expectedRowCount, $actualData, 'Row count should match test data');

        // Verify column structure using reflection
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $actualColumns = $ref->getColumnsDefinitions();

        // Verify each column's type and nullability
        // Note: Nullability handling differs between VIEW and FULL imports due to BigQuery limitations
        foreach ($columns as $expectedColumn) {
            $actualColumn = null;
            foreach ($actualColumns as $col) {
                if ($col->getColumnName() === $expectedColumn['name']) {
                    $actualColumn = $col;
                    break;
                }
            }
            $this->assertNotNull($actualColumn, sprintf('Column %s should exist', $expectedColumn['name']));

            $columnDef = $actualColumn->getColumnDefinition();
            $this->assertSame(
                $this->normalizeType($expectedColumn['type']),
                $columnDef->getType(),
                sprintf('Column %s type should be %s', $expectedColumn['name'], $expectedColumn['type']),
            );

            /*
             * NULLABILITY VALIDATION
             * ----------------------
             * BigQuery handles nullability differently for views vs tables:
             *
             * VIEW (logical view):
             *   - All columns are nullable regardless of source table constraints
             *   - This is because views are virtual/read-only with no data modification
             *   - NOT NULL constraints only apply to physical tables during INSERT/UPDATE
             *   - View queries could return NULLs from JOINs, CASE statements, etc.
             *
             * FULL (physical table):
             *   - NOT NULL constraints are preserved during import
             *   - Physical tables enforce constraints during data modifications
             *   - The destination table will have the same nullability as the source
             *
             * This is documented BigQuery behavior, not a bug.
             * Reference: https://cloud.google.com/bigquery/docs/views-intro
             */
            if ($isView) {
                // For VIEW imports: Assert all columns ARE nullable (expected BigQuery behavior)
                $this->assertTrue(
                    $columnDef->isNullable(),
                    sprintf(
                        'Column %s in VIEW should be nullable (BigQuery views do not preserve NOT NULL constraints)',
                        $expectedColumn['name'],
                    ),
                );
            } else {
                // For FULL imports: Assert nullability matches source configuration
                $this->assertSame(
                    $expectedColumn['nullable'],
                    $columnDef->isNullable(),
                    sprintf(
                        'Column %s nullability should be %s in physical table',
                        $expectedColumn['name'],
                        $expectedColumn['nullable'] ? 'true' : 'false',
                    ),
                );
            }
        }

        // Cleanup
        $this->cleanupTableOrView($bucketDatabaseName, $destinationTableName, $isView, $bqClient);
    }

    /**
     * Normalize BigQuery type aliases to their canonical forms returned by BigQuery.
     *
     * BigQuery supports multiple type aliases for compatibility, but internally
     * normalizes them to canonical type names. This method maps the type constants
     * used in test data to what BigQuery actually returns via reflection.
     *
     * For example, all integer type aliases (INT, INT64, BIGINT, TINYINT, SMALLINT)
     * are normalized to the canonical 'INTEGER' type in BigQuery.
     *
     * @param string $type The Bigquery type constant (e.g., Bigquery::TYPE_INT)
     * @return string The canonical BigQuery type name (e.g., 'INTEGER')
     */
    private function normalizeType(string $type): string
    {
        // Map Bigquery datatype constants to what BigQuery actually returns
        // All integer types (INT, INT64, BIGINT) normalize to INTEGER in BigQuery
        $typeMap = [
            Bigquery::TYPE_INT => 'INTEGER',
            Bigquery::TYPE_INT64 => 'INTEGER',
            Bigquery::TYPE_BIGINT => 'INTEGER',
        ];

        return $typeMap[$type] ?? $type;
    }

    /**
     * @param array<array{name: string, type: string, nullable: bool}> $columns
     */
    private function buildColumnCollection(array $columns): ColumnCollection
    {
        $columnObjects = [];
        foreach ($columns as $column) {
            $columnObjects[] = new BigqueryColumn(
                $column['name'],
                new Bigquery(
                    $column['type'],
                    ['nullable' => $column['nullable']],
                ),
            );
        }

        return new ColumnCollection($columnObjects);
    }

    /**
     * @param array<array{name: string, type: string, nullable: bool}> $columns
     */
    private function createSourceTableWithColumns(
        string $schemaName,
        string $tableName,
        array $columns,
        BigQueryClient $bqClient,
    ): void {
        $columnCollection = $this->buildColumnCollection($columns);

        $tableDef = new BigqueryTableDefinition(
            $schemaName,
            $tableName,
            false,
            $columnCollection,
            [],
        );

        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableDef->getSchemaName(),
            $tableDef->getTableName(),
            $tableDef->getColumnsDefinitions(),
            $tableDef->getPrimaryKeysNames(),
        );

        $bqClient->runQuery($bqClient->query($sql));
    }

    /**
     * @param array<array{name: string, type: string, nullable: bool}> $columns
     * @param array<array<string|null>> $testData
     */
    private function insertTestData(
        string $schemaName,
        string $tableName,
        array $columns,
        array $testData,
        BigQueryClient $bqClient,
    ): void {
        $columnNames = array_map(fn($col) => $col['name'], $columns);
        $quotedColumnNames = array_map(
            fn($name) => BigqueryQuote::quoteSingleIdentifier($name),
            $columnNames,
        );

        foreach ($testData as $row) {
            $quotedValues = [];
            foreach ($row as $value) {
                if ($value === null) {
                    $quotedValues[] = 'NULL';
                } elseif (is_string($value)) {
                    // Quote string values
                    $quotedValues[] = BigqueryQuote::quote($value);
                } else {
                    // Numeric types (int, float) are not quoted
                    $quotedValues[] = (string) $value;
                }
            }

            $sql = sprintf(
                'INSERT %s.%s (%s) VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($schemaName),
                BigqueryQuote::quoteSingleIdentifier($tableName),
                implode(', ', $quotedColumnNames),
                implode(', ', $quotedValues),
            );

            $bqClient->runQuery($bqClient->query($sql));
        }
    }

    private function cleanupTableOrView(
        string $schemaName,
        string $tableName,
        bool $isView,
        BigQueryClient $bqClient,
    ): void {
        $objectType = $isView ? 'VIEW' : 'TABLE';
        $sql = sprintf(
            'DROP %s %s.%s',
            $objectType,
            BigqueryQuote::quoteSingleIdentifier($schemaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        );

        $bqClient->runQuery($bqClient->query($sql));
    }
}
