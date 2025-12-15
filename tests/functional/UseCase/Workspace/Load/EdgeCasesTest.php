<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace\Load;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

/**
 * Tests for edge cases and special scenarios
 * @group Workspace
 */
class EdgeCasesTest extends BaseImportTestCase
{
    /**
     * @return Generator<string,array{int}>
     */
    public function importTypeProvider(): Generator
    {
        yield 'FULL' => [ImportOptions\ImportType::FULL];
        yield 'INCREMENTAL' => [ImportOptions\ImportType::INCREMENTAL];
        yield 'VIEW' => [ImportOptions\ImportType::VIEW];
        yield 'CLONE' => [ImportOptions\ImportType::PBCLONE];
    }

    /**
     * Test importing from empty source table (0 rows)
     * Should succeed with 0 imported rows
     * @dataProvider importTypeProvider
     */
    public function testImportFromEmptySourceTable(int $importType): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_empty_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create empty source table (no rows)
        $this->createEmptyTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // Create destination table (for FULL/INCREMENTAL) or skip for VIEW/CLONE
        if (in_array($importType, [ImportOptions\ImportType::FULL, ImportOptions\ImportType::INCREMENTAL], true)) {
            $this->createEmptyTable($bucketDatabaseName, $destinationTableName, $bqClient);
        }

        // Import from empty source
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
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
                ->setImportType($importType)
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

        // Should succeed with 0 imported rows
        if ($importType === ImportOptions\ImportType::VIEW) {
            // VIEWs always return 0
            $this->assertSame(0, $response->getImportedRowsCount());
        } else {
            // FULL, INCREMENTAL, CLONE should also return 0 for empty source
            $this->assertSame(0, $response->getImportedRowsCount());
        }

        // Verify destination exists and is empty (or view is queryable)
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertTrue($ref->exists());

        if ($importType !== ImportOptions\ImportType::VIEW) {
            // Regular tables should have 0 rows
            $this->assertSame(0, $ref->getRowsCount());
        }
    }

    /**
     * Test table names with special characters (properly quoted)
     */
    public function testTableNamesWithSpecialCharacters(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $baseTableName = $this->getTestHash();

        // Create source table with special characters in name
        // createTableWithSpecialChars appends '_with-dash' to the name
        $sourceTableName = $baseTableName . '_source_with-dash';
        $sourceDef = $this->createTableWithSpecialChars($bucketDatabaseName, $baseTableName . '_source', $bqClient);

        // Create destination with dashes in name
        $destinationTableName = $baseTableName . '_dest-with-dash';
        $destDef = $this->createEmptyTable($bucketDatabaseName, $destinationTableName, $bqClient);

        // Import with special character names
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
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

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Should succeed - BigQuery handles quoted identifiers
        $this->assertSame(1, $response->getImportedRowsCount());
    }

    /**
     * Test column names with special characters (properly quoted)
     */
    public function testColumnNamesWithSpecialCharacters(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $baseTableName = $this->getTestHash();

        // Create source with special char column (appends '_with-dash' to table name)
        $sourceTableName = $baseTableName . '_source_with-dash';
        $sourceDef = $this->createTableWithSpecialChars($bucketDatabaseName, $baseTableName . '_source', $bqClient);

        // Create destination with same special char column
        $destinationTableName = $baseTableName . '_dest_with-dash';
        $destDef = $this->createTableWithSpecialChars($bucketDatabaseName, $baseTableName . '_dest', $bqClient);

        // Import with column mapping on special char column names
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col-with-dash')
            ->setDestinationColumnName('col-with-dash');

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

        // Should succeed with special char column names
        $this->assertSame(1, $response->getImportedRowsCount());
        $importedColumns = iterator_to_array($response->getImportedColumns());
        $this->assertContains('col-with-dash', $importedColumns);
    }

    /**
     * Test VIEW import from empty source table
     * Should create view that returns 0 rows when queried
     */
    public function testViewFromEmptySourceTable(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_empty_source';
        $viewName = $this->getTestHash() . '_view';

        // Create empty source table
        $this->createEmptyTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // Create VIEW from empty source
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($viewName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::VIEW),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // VIEW should be created successfully
        $this->assertTrue($this->verifyTableIsView($bqClient, $bucketDatabaseName, $viewName));
        $this->assertSame(0, $response->getImportedRowsCount());

        // Query view should return 0 rows
        $rowCount = $this->getTableRowCount($bqClient, $bucketDatabaseName, $viewName);
        $this->assertSame(0, $rowCount);
    }
}
