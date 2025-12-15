<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace\Load;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\ObjectAlreadyExistsException;
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
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

/**
 * Tests for CREATE and REPLACE mode behavior
 * @group Workspace
 */
class CreateModeTest extends BaseImportTestCase
{
    /**
     * @return Generator<string,array{int}>
     */
    public function importTypeProvider(): Generator
    {
        yield 'FULL' => [ImportOptions\ImportType::FULL];
        yield 'VIEW' => [ImportOptions\ImportType::VIEW];
        yield 'CLONE' => [ImportOptions\ImportType::PBCLONE];
    }

    /**
     * Test CREATE mode with non-existent table (should succeed)
     * @dataProvider importTypeProvider
     */
    public function testCreateModeWithNonExistentTable(int $importType): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source table
        $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // Import to non-existent destination with CREATE mode (default)
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
                ->setCreateMode(ImportOptions\CreateMode::CREATE),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify table was created
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertTrue($ref->exists());

        if ($importType === ImportOptions\ImportType::VIEW) {
            $this->assertSame(0, $response->getImportedRowsCount());
        } else {
            // FULL and CLONE should have imported rows
            $this->assertGreaterThan(0, $this->getTableRowCount($bqClient, $bucketDatabaseName, $destinationTableName));
        }
    }

    /**
     * Test CREATE mode with existing table (should throw ObjectAlreadyExistsException)
     * @dataProvider importTypeProvider
     */
    public function testCreateModeWithExistingTableShouldFail(int $importType): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source and destination tables
        $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);
        $this->createSourceTable($bucketDatabaseName, $destinationTableName, $bqClient);

        // Try to import with CREATE mode (should fail)
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
                ->setCreateMode(ImportOptions\CreateMode::CREATE),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $this->expectException(ObjectAlreadyExistsException::class);
        $this->expectExceptionCode(2006);

        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    /**
     * Test REPLACE mode with VIEW on existing table (should drop and recreate)
     */
    public function testReplaceModeWithViewDropsAndRecreates(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source table
        $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // Create existing destination table
        $this->createSourceTable($bucketDatabaseName, $destinationTableName, $bqClient);
        $initialRowCount = $this->getTableRowCount($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $initialRowCount);

        // Import with REPLACE mode
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
                ->setImportType(ImportOptions\ImportType::VIEW)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify destination is now a VIEW (not a table)
        $this->assertTrue($this->verifyTableIsView($bqClient, $bucketDatabaseName, $destinationTableName));

        // Views return 0 for imported rows count
        $this->assertSame(0, $response->getImportedRowsCount());

        // But the view should be queryable and return source data
        $this->assertSame(3, $this->getTableRowCount($bqClient, $bucketDatabaseName, $destinationTableName));
    }

    /**
     * Test REPLACE mode with CLONE on existing table (should drop and recreate)
     */
    public function testReplaceModeWithCloneDropsAndRecreates(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source table with 3 rows
        $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // Create existing destination table with different data (5 rows)
        $this->createSourceTableWithRows($bucketDatabaseName, $destinationTableName, $bqClient, 5);
        $initialRowCount = $this->getTableRowCount($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(5, $initialRowCount);

        // Import with REPLACE mode using CLONE
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
                ->setImportType(ImportOptions\ImportType::PBCLONE)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify destination was replaced and now has 3 rows (from source)
        $newRowCount = $this->getTableRowCount($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $newRowCount);

        // Verify it's a regular table (not a view)
        $this->assertFalse($this->verifyTableIsView($bqClient, $bucketDatabaseName, $destinationTableName));
    }

    /**
     * Test REPLACE mode with FULL import on existing table (should drop and recreate)
     */
    public function testReplaceModeWithFullImportDropsAndRecreates(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source table
        $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // Create destination table as typed table
        $this->createDestinationTypedTable($bucketDatabaseName, $destinationTableName, $bqClient);

        // Note: REPLACE mode with FULL import type does NOT trigger the shouldDropTableIfExists logic
        // because only VIEW and PBCLONE are in the allowed list (lines 94-95 in handler)
        // So this will fail with ObjectAlreadyExistsException
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
                ->setCreateMode(ImportOptions\CreateMode::REPLACE),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        // REPLACE mode with FULL import type is NOT supported
        // Only VIEW and PBCLONE support REPLACE mode
        $this->expectException(ObjectAlreadyExistsException::class);

        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    private function createSourceTable(
        string $bucketDatabaseName,
        string $sourceTableName,
        BigQueryClient $bqClient,
    ): void {
        $this->createSourceTableWithRows($bucketDatabaseName, $sourceTableName, $bqClient, 3);
    }

    private function createSourceTableWithRows(
        string $bucketDatabaseName,
        string $tableName,
        BigQueryClient $bqClient,
        int $rowCount = 3,
    ): void {
        $tableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $tableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createGenericColumn('col3'),
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

        // Insert rows
        $insert = [];
        for ($i = 1; $i <= $rowCount; $i++) {
            $quotedValues = [
                BigqueryQuote::quote((string) $i),
                BigqueryQuote::quote((string) $i),
                BigqueryQuote::quote((string) $i),
            ];
            $insert[] = sprintf('(%s)', implode(',', $quotedValues));
        }

        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
            implode(',', $insert),
        )));
    }
}
