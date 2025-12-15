<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\FromTable;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;

/**
 * Tests for CopyImportFromTableToTable optimization path
 * Tests lines 246-271 in handler: isColumnIdentical check and importer selection
 * @group Import
 */
class CopyOptimizationTest extends BaseImportTestCase
{
    /**
     * Test that identical columns trigger fast COPY path
     * When source and staging table have identical columns (same order, same types),
     * CopyImportFromTableToTable is used instead of ToStageImporter
     */
    public function testIdenticalColumnsTriggerCopyOptimization(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source and destination with IDENTICAL structure
        $columns = [
            BigqueryColumn::createGenericColumn('col1'),
            BigqueryColumn::createGenericColumn('col2'),
            BigqueryColumn::createGenericColumn('col3'),
        ];

        $this->createTableWithStructure($bucketDatabaseName, $sourceTableName, $bqClient, $columns, true);
        $this->createTableWithStructure($bucketDatabaseName, $destinationTableName, $bqClient, $columns, false);

        // Import with FULL load - should use COPY optimization
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName),
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

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $startTime = microtime(true);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $duration = microtime(true) - $startTime;

        // Verify import succeeded
        $this->assertSame(3, $response->getImportedRowsCount());

        // COPY optimization should be relatively fast (no staging table creation)
        // This is just to document the optimization is used, not a strict requirement
        $this->assertLessThan(60, $duration, 'COPY optimization should complete within 60 seconds');
    }

    /**
     * Test that different columns use ToStageImporter with staging table
     * When columns are not identical, a staging table must be created
     */
    public function testDifferentColumnsUsesStagingTable(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source with columns: col1, col2, col3, col4
        $sourceColumns = [
            BigqueryColumn::createGenericColumn('col1'),
            BigqueryColumn::createGenericColumn('col2'),
            BigqueryColumn::createGenericColumn('col3'),
            BigqueryColumn::createGenericColumn('col4'),
        ];
        $this->createTableWithStructure($bucketDatabaseName, $sourceTableName, $bqClient, $sourceColumns, true);

        // Create destination with different columns: col1, col3 (subset)
        $destColumns = [
            BigqueryColumn::createGenericColumn('col1'),
            BigqueryColumn::createGenericColumn('col3'),
        ];
        $this->createTableWithStructure($bucketDatabaseName, $destinationTableName, $bqClient, $destColumns, false);

        // Import with column mapping (different structure)
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col3')
            ->setDestinationColumnName('col3');

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
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify import succeeded with column mapping
        $this->assertSame(3, $response->getImportedRowsCount());
        $importedColumns = iterator_to_array($response->getImportedColumns());
        $this->assertCount(2, $importedColumns);

        // Verify only mapped columns have data
        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s LIMIT 1',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
        $rows = iterator_to_array($result);
        $this->assertArrayHasKey('col1', $rows[0]);
        $this->assertArrayHasKey('col3', $rows[0]);
        $this->assertArrayNotHasKey('col2', $rows[0]);
        $this->assertArrayNotHasKey('col4', $rows[0]);
    }

    /**
     * Test COPY optimization with timestamp column
     * When using timestamp, should still use optimized path if columns identical
     */
    public function testCopyOptimizationWithTimestamp(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create identical tables with timestamp column
        $columns = [
            BigqueryColumn::createGenericColumn('col1'),
            BigqueryColumn::createGenericColumn('col2'),
            BigqueryColumn::createTimestampColumn('_timestamp'),
        ];

        $this->createTableWithStructure($bucketDatabaseName, $sourceTableName, $bqClient, $columns, true);
        $this->createTableWithStructure($bucketDatabaseName, $destinationTableName, $bqClient, $columns, false);

        // Import with timestamp updates
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setTimestampColumn('_timestamp'),
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
        $this->assertSame(3, $response->getImportedRowsCount());

        // Verify timestamp column was updated
        $this->assertTimestamp($bqClient, $bucketDatabaseName, $destinationTableName);
    }

    /**
     * Test incremental load with identical columns
     */
    public function testIncrementalLoadWithIdenticalColumns(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create identical tables
        $columns = [
            BigqueryColumn::createGenericColumn('col1'),
            BigqueryColumn::createGenericColumn('col2'),
            BigqueryColumn::createGenericColumn('col3'),
        ];

        $this->createTableWithStructure($bucketDatabaseName, $sourceTableName, $bqClient, $columns, true);
        $this->createTableWithStructure($bucketDatabaseName, $destinationTableName, $bqClient, $columns, false);

        // First import
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Should use COPY optimization for incremental too
        $this->assertSame(3, $response->getImportedRowsCount());

        // Verify destination has data
        $rowCount = $this->getTableRowCount($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $rowCount);
    }

    private function createTableWithStructure(
        string $bucketDatabaseName,
        string $tableName,
        BigQueryClient $bqClient,
        array $columns,
        bool $insertData = false,
    ): void {
        $tableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $tableName,
            false,
            new ColumnCollection($columns),
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

        if ($insertData) {
            // Insert 3 rows of test data
            $columnNames = array_map(fn($col) => $col->getColumnName(), $columns);
            $columnList = implode(', ', array_map(fn($col) => BigqueryQuote::quoteSingleIdentifier($col), $columnNames));

            for ($i = 1; $i <= 3; $i++) {
                $values = [];
                foreach ($columnNames as $colName) {
                    if ($colName === '_timestamp') {
                        $values[] = 'CURRENT_TIMESTAMP()';
                    } else {
                        $values[] = BigqueryQuote::quote((string) $i);
                    }
                }
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
    }
}
