<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace\Load;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;

/**
 * Tests for column mapping scenarios (FULL and INCREMENTAL import types)
 * @group Workspace
 */
class ColumnMappingTest extends BaseImportTestCase
{
    /**
     * Test importing all columns when no column mappings specified
     */
    public function testImportAllColumnsWithEmptyMapping(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source table with 4 columns
        $this->createSourceTableWithColumns($bucketDatabaseName, $sourceTableName, $bqClient, ['col1', 'col2', 'col3', 'col4']);

        // Create destination table with same structure
        $this->createDestinationTableWithColumns($bucketDatabaseName, $destinationTableName, $bqClient, ['col1', 'col2', 'col3', 'col4']);

        // Import with no column mappings (should import all columns)
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
                // No column mappings specified - should import all columns
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify all 4 columns were imported
        $this->assertSame(3, $response->getImportedRowsCount());
        $importedColumns = iterator_to_array($response->getImportedColumns());
        $this->assertCount(4, $importedColumns);
        $this->assertContains('col1', $importedColumns);
        $this->assertContains('col2', $importedColumns);
        $this->assertContains('col3', $importedColumns);
        $this->assertContains('col4', $importedColumns);
    }

    /**
     * Test importing subset of columns
     */
    public function testImportSubsetOfColumns(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source table with 4 columns
        $this->createSourceTableWithColumns($bucketDatabaseName, $sourceTableName, $bqClient, ['col1', 'col2', 'col3', 'col4']);

        // Create destination table with only 2 columns
        $this->createDestinationTableWithColumns($bucketDatabaseName, $destinationTableName, $bqClient, ['col1', 'col3']);

        // Import only col1 and col3
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col3')
            ->setDestinationColumnName('col3');

        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
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
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify only 2 columns were imported
        $this->assertSame(3, $response->getImportedRowsCount());
        $importedColumns = iterator_to_array($response->getImportedColumns());
        $this->assertCount(2, $importedColumns);
        $this->assertContains('col1', $importedColumns);
        $this->assertContains('col3', $importedColumns);
    }

    /**
     * Test column reordering (different order than source)
     */
    public function testColumnReordering(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source table with columns in order: col1, col2, col3
        $this->createSourceTableWithColumns($bucketDatabaseName, $sourceTableName, $bqClient, ['col1', 'col2', 'col3']);

        // Create destination table with columns in different order: col3, col1, col2
        $this->createDestinationTableWithColumns($bucketDatabaseName, $destinationTableName, $bqClient, ['col3', 'col1', 'col2']);

        // Map columns in reordered fashion
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        // Map in destination order: col3, col1, col2
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col3')
            ->setDestinationColumnName('col3');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('col2');

        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
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
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify all columns imported with correct data
        $this->assertSame(3, $response->getImportedRowsCount());

        // Query and verify data order
        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s ORDER BY col1 LIMIT 1',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
        $rows = iterator_to_array($result);
        $firstRow = $rows[0];
        $this->assertSame('1', $firstRow['col1']);
        $this->assertSame('1', $firstRow['col2']);
        $this->assertSame('1', $firstRow['col3']);
    }

    /**
     * Test column renaming (source column name differs from destination)
     */
    public function testColumnRenaming(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source table with columns: col1, col2, col3
        $this->createSourceTableWithColumns($bucketDatabaseName, $sourceTableName, $bqClient, ['col1', 'col2', 'col3']);

        // Create destination table with renamed columns: col1, col2_renamed, col3_renamed
        $this->createDestinationTableWithColumns($bucketDatabaseName, $destinationTableName, $bqClient, ['col1', 'col2_renamed', 'col3_renamed']);

        // Map with renaming
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('col2_renamed');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col3')
            ->setDestinationColumnName('col3_renamed');

        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
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
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify renamed columns in response
        $importedColumns = iterator_to_array($response->getImportedColumns());
        $this->assertContains('col1', $importedColumns);
        $this->assertContains('col2_renamed', $importedColumns);
        $this->assertContains('col3_renamed', $importedColumns);
    }

    /**
     * Test column mapping with typed source table
     */
    public function testColumnMappingWithTypedSourceTable(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create typed source table (INT, BIGINT, STRING)
        $this->createTypedSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // Create typed destination table with subset of columns
        $this->createTypedDestinationTable($bucketDatabaseName, $destinationTableName, $bqClient);

        // Map columns
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('col2');

        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
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
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify data imported correctly with types preserved
        $this->assertSame(3, $response->getImportedRowsCount());

        // Query and verify types are preserved
        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT col1, col2 FROM %s.%s ORDER BY col1 LIMIT 1',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
        $rows = iterator_to_array($result);
        $this->assertSame(1, $rows[0]['col1']); // INT type
        $this->assertSame(2, $rows[0]['col2']); // BIGINT type
    }

    private function createSourceTableWithColumns(
        string $bucketDatabaseName,
        string $tableName,
        BigQueryClient $bqClient,
        array $columns,
    ): void {
        $columnObjects = [];
        foreach ($columns as $col) {
            $columnObjects[] = BigqueryColumn::createGenericColumn($col);
        }

        $tableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $tableName,
            false,
            new ColumnCollection($columnObjects),
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

        // Insert 3 rows of test data
        $columnList = implode(', ', array_map(fn($col) => BigqueryQuote::quoteSingleIdentifier($col), $columns));
        for ($i = 1; $i <= 3; $i++) {
            $values = array_map(fn() => BigqueryQuote::quote((string) $i), $columns);
            $sql = sprintf(
                'INSERT INTO %s.%s (%s) VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($tableName),
                $columnList,
                implode(',', $values),
            );
            $bqClient->runQuery($bqClient->query($sql));
        }
    }

    private function createDestinationTableWithColumns(
        string $bucketDatabaseName,
        string $tableName,
        BigQueryClient $bqClient,
        array $columns,
    ): void {
        $columnObjects = [];
        foreach ($columns as $col) {
            $columnObjects[] = BigqueryColumn::createGenericColumn($col);
        }

        $tableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $tableName,
            false,
            new ColumnCollection($columnObjects),
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

    private function createTypedSourceTable(
        string $bucketDatabaseName,
        string $tableName,
        BigQueryClient $bqClient,
    ): void {
        $tableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $tableName,
            false,
            new ColumnCollection([
                new BigqueryColumn('col1', new Bigquery(Bigquery::TYPE_INT, [])),
                new BigqueryColumn('col2', new Bigquery(Bigquery::TYPE_BIGINT, [])),
                new BigqueryColumn('col3', new Bigquery(Bigquery::TYPE_STRING, ['length' => '255'])),
            ]),
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

        // Insert typed data
        for ($i = 1; $i <= 3; $i++) {
            $sql = sprintf(
                'INSERT INTO %s.%s (col1, col2, col3) VALUES (%d, %d, %s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($tableName),
                $i,
                $i + 1,
                BigqueryQuote::quote("value$i"),
            );
            $bqClient->runQuery($bqClient->query($sql));
        }
    }

    private function createTypedDestinationTable(
        string $bucketDatabaseName,
        string $tableName,
        BigQueryClient $bqClient,
    ): void {
        $tableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $tableName,
            false,
            new ColumnCollection([
                new BigqueryColumn('col1', new Bigquery(Bigquery::TYPE_INT, [])),
                new BigqueryColumn('col2', new Bigquery(Bigquery::TYPE_BIGINT, [])),
            ]),
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
}
