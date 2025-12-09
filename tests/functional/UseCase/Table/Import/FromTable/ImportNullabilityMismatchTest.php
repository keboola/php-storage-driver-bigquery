<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\FromTable;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use InvalidArgumentException;
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
 * Tests that nullability differences don't cause validation failures during incremental imports.
 *
 * REGRESSION TEST for bug:
 * Previously, the validation in ImportDestinationManager::validateIncrementalDestination()
 * required exact nullability match, which caused failures for valid scenarios involving VIEWs:
 *
 * Flow that previously failed:
 * 1. Original table with NOT NULL columns
 * 2. CREATE VIEW from table → columns become nullable (BigQuery limitation)
 * 3. CREATE TABLE from VIEW → inherits nullable columns
 * 4. INCREMENTAL import to original table → FAILED on nullability validation
 *
 * This test ensures that nullability differences are allowed, as BigQuery enforces
 * NOT NULL constraints at INSERT time, not during schema validation.
 *
 * @group Import
 */
class ImportNullabilityMismatchTest extends BaseImportTestCase
{
    /**
     * Test incremental import from nullable source to NOT NULL destination.
     *
     * This simulates the real-world scenario where:
     * - Source table has nullable columns (e.g., created from a VIEW)
     * - Destination table has NOT NULL columns (original typed table)
     * - Incremental import should succeed despite nullability difference
     *
     * The validation should allow this because BigQuery will enforce
     * NOT NULL at INSERT time if actual NULL values are present.
     */
    public function testIncrementalImportNullableToNotNullSucceeds(): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source_nullable';
        $destinationTableName = $this->getTestHash() . '_dest_not_null';

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // Create source table with NULLABLE columns (simulating table created from VIEW)
        $this->createTableWithNullability(
            $bqClient,
            $bucketDatabaseName,
            $sourceTableName,
            [
                new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_INT, ['nullable' => true])),
                new BigqueryColumn('name', new Bigquery(Bigquery::TYPE_STRING, ['nullable' => true])),
                new BigqueryColumn('amount', new Bigquery(Bigquery::TYPE_NUMERIC, ['nullable' => true])),
            ],
            ['id'],
        );

        // Insert test data (no NULLs, so import to NOT NULL destination will succeed)
        $this->insertTestData($bqClient, $bucketDatabaseName, $sourceTableName, [
            [1, 'Alice', 100],
            [2, 'Bob', 200],
            [3, 'Charlie', 300],
        ]);

        // Create destination table with NOT NULL columns (original typed table)
        $this->createTableWithNullability(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            [
                new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_INT, ['nullable' => false])),
                new BigqueryColumn('name', new Bigquery(Bigquery::TYPE_STRING, ['nullable' => false])),
                new BigqueryColumn('amount', new Bigquery(Bigquery::TYPE_NUMERIC, ['nullable' => false])),
            ],
            ['id'],
        );

        // Pre-populate destination with some data
        $this->insertTestData($bqClient, $bucketDatabaseName, $destinationTableName, [
            [1, 'Alice Old', 50],
            [4, 'David', 400],
        ]);

        // Execute INCREMENTAL import with UPDATE_DUPLICATES
        // This should succeed despite nullability mismatch
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'name', 'amount'] as $colName) {
            $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($colName)
                ->setDestinationColumnName($colName);
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

        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'id';

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns)
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify import succeeded
        $this->assertInstanceOf(TableImportResponse::class, $response);
        $this->assertSame(3, $response->getImportedRowsCount());

        // Verify data in destination
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(4, $ref->getRowsCount()); // 3 from source + 1 existing (4 updated)

        // Verify NOT NULL constraints are still enforced in destination
        $destColumns = $ref->getColumnsDefinitions();
        foreach ($destColumns as $col) {
            if (in_array($col->getColumnName(), ['id', 'name', 'amount'], true)) {
                $this->assertFalse(
                    $col->getColumnDefinition()->isNullable(),
                    sprintf('Destination column %s should remain NOT NULL', $col->getColumnName()),
                );
            }
        }

        // Cleanup
        $this->dropTable($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTable($bqClient, $bucketDatabaseName, $destinationTableName);
    }

    /**
     * Test incremental import from NOT NULL source to nullable destination.
     *
     * This is the reverse scenario and should also succeed.
     */
    public function testIncrementalImportNotNullToNullableSucceeds(): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source_not_null';
        $destinationTableName = $this->getTestHash() . '_dest_nullable';

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // Create source table with NOT NULL columns
        $this->createTableWithNullability(
            $bqClient,
            $bucketDatabaseName,
            $sourceTableName,
            [
                new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_INT, ['nullable' => false])),
                new BigqueryColumn('name', new Bigquery(Bigquery::TYPE_STRING, ['nullable' => false])),
                new BigqueryColumn('value', new Bigquery(Bigquery::TYPE_FLOAT64, ['nullable' => false])),
            ],
            ['id'],
        );

        $this->insertTestData($bqClient, $bucketDatabaseName, $sourceTableName, [
            [1, 'Item A', 10.5],
            [2, 'Item B', 20.0],
        ]);

        // Create destination table with NULLABLE columns
        $this->createTableWithNullability(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            [
                new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_INT, ['nullable' => true])),
                new BigqueryColumn('name', new Bigquery(Bigquery::TYPE_STRING, ['nullable' => true])),
                new BigqueryColumn('value', new Bigquery(Bigquery::TYPE_FLOAT64, ['nullable' => true])),
            ],
            ['id'],
        );

        // Execute import
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'name', 'value'] as $colName) {
            $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($colName)
                ->setDestinationColumnName($colName);
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

        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'id';

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns)
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify import succeeded
        $this->assertInstanceOf(TableImportResponse::class, $response);
        $this->assertSame(2, $response->getImportedRowsCount());

        // Cleanup
        $this->dropTable($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTable($bqClient, $bucketDatabaseName, $destinationTableName);
    }

    /**
     * Test the complete VIEW flow that previously failed.
     *
     * This test simulates the exact flow described in the bug report:
     * 1. Typed table with NOT NULL → VIEW → Table (nullable) → Incremental import back
     */
    public function testViewFlowWithNullabilityMismatch(): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $originalTableName = $this->getTestHash() . '_original';
        $viewName = $this->getTestHash() . '_view';
        $intermediateTableName = $this->getTestHash() . '_from_view';

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // Step 1: Create original typed table with NOT NULL columns
        $this->createTableWithNullability(
            $bqClient,
            $bucketDatabaseName,
            $originalTableName,
            [
                new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_INT, ['nullable' => false])),
                new BigqueryColumn('category', new Bigquery(Bigquery::TYPE_STRING, ['nullable' => false])),
                new BigqueryColumn('quantity', new Bigquery(Bigquery::TYPE_INT, ['nullable' => false])),
            ],
            ['id'],
        );

        $this->insertTestData($bqClient, $bucketDatabaseName, $originalTableName, [
            [1, 'Electronics', 100],
            [2, 'Books', 200],
        ]);

        // Step 2: Create VIEW from original table
        $sql = sprintf(
            'CREATE VIEW %s.%s AS SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($originalTableName),
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Verify VIEW has nullable columns (BigQuery limitation)
        $viewRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $viewName);
        foreach ($viewRef->getColumnsDefinitions() as $col) {
            $this->assertTrue(
                $col->getColumnDefinition()->isNullable(),
                sprintf('VIEW column %s should be nullable', $col->getColumnName()),
            );
        }

        // Step 3: Create table from VIEW (inherits nullable columns)
        $sql = sprintf(
            'CREATE TABLE %s.%s AS SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($intermediateTableName),
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Verify intermediate table has nullable columns
        $intermediateRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $intermediateTableName);
        foreach ($intermediateRef->getColumnsDefinitions() as $col) {
            $this->assertTrue(
                $col->getColumnDefinition()->isNullable(),
                sprintf('Intermediate table column %s should be nullable', $col->getColumnName()),
            );
        }

        // Add new data to intermediate table
        $this->insertTestData($bqClient, $bucketDatabaseName, $intermediateTableName, [
            [3, 'Toys', 300],
        ]);

        // Step 4: INCREMENTAL import from intermediate (nullable) back to original (NOT NULL)
        // This is where the bug occurred - validation should now allow this
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'category', 'quantity'] as $colName) {
            $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($colName)
                ->setDestinationColumnName($colName);
        }

        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($intermediateTableName)
                ->setColumnMappings($columnMappings),
        );

        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($originalTableName),
        );

        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'id';

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns)
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        // This should succeed (previously failed with nullability validation error)
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify import succeeded
        $this->assertInstanceOf(TableImportResponse::class, $response);
        $this->assertSame(3, $response->getImportedRowsCount());

        // Verify final data
        $finalRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $originalTableName);
        $this->assertSame(3, $finalRef->getRowsCount());

        // Verify original table still has NOT NULL constraints
        foreach ($finalRef->getColumnsDefinitions() as $col) {
            if (in_array($col->getColumnName(), ['id', 'category', 'quantity'], true)) {
                $this->assertFalse(
                    $col->getColumnDefinition()->isNullable(),
                    sprintf('Original table column %s should remain NOT NULL', $col->getColumnName()),
                );
            }
        }

        // Cleanup
        $this->dropTable($bqClient, $bucketDatabaseName, $originalTableName);
        $sql = sprintf(
            'DROP VIEW %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
        );
        $bqClient->runQuery($bqClient->query($sql));
        $this->dropTable($bqClient, $bucketDatabaseName, $intermediateTableName);
    }

    /**
     * Helper: Create a table with specified columns and primary keys.
     *
     * Note: Named createTableWithNullability to avoid conflict with base class methods
     *
     * @param BigqueryColumn[] $columns
     * @param string[] $primaryKeys
     */
    private function createTableWithNullability(
        BigQueryClient $bqClient,
        string $schemaName,
        string $tableName,
        array $columns,
        array $primaryKeys,
    ): void {
        $tableDef = new BigqueryTableDefinition(
            $schemaName,
            $tableName,
            false,
            new ColumnCollection($columns),
            $primaryKeys,
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
     * Helper: Insert data into a table.
     *
     * Note: Named insertTestData to avoid potential conflicts
     *
     * @param array<array<mixed>> $rows
     */
    private function insertTestData(
        BigQueryClient $bqClient,
        string $schemaName,
        string $tableName,
        array $rows,
    ): void {
        foreach ($rows as $row) {
            $quotedValues = [];
            foreach ($row as $value) {
                if (is_string($value)) {
                    $quotedValues[] = BigqueryQuote::quote($value);
                } elseif ($value === null) {
                    $quotedValues[] = 'NULL';
                } elseif (is_scalar($value)) {
                    $quotedValues[] = (string) $value;
                } else {
                    throw new InvalidArgumentException('Unsupported value type');
                }
            }

            $sql = sprintf(
                'INSERT %s.%s VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($schemaName),
                BigqueryQuote::quoteSingleIdentifier($tableName),
                implode(', ', $quotedValues),
            );

            $bqClient->runQuery($bqClient->query($sql));
        }
    }

    /**
     * Helper: Drop a table.
     *
     */
    private function dropTable(
        BigQueryClient $bqClient,
        string $schemaName,
        string $tableName,
    ): void {
        $sql = sprintf(
            'DROP TABLE %s.%s',
            BigqueryQuote::quoteSingleIdentifier($schemaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        );

        $bqClient->runQuery($bqClient->query($sql));
    }
}
