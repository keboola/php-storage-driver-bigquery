<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\FromTable;

use Generator;
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
 * @group Import
 */
class IncrementalImportTableFromTableTest extends BaseImportTestCase
{
    /**
     * @dataProvider typedTablesProvider
     *
     * Incremental load to storage from workspace
     * This is output mapping, timestamp is updated
     */
    public function testImportTableFromTableIncrementalLoad(bool $isTypedTable): void
    {
        // typed tables have to have same structure, but string tables can do the mapping
        $sourceExtraColumn = $isTypedTable ? 'col3' : 'colX';

        $sourceTableName = $this->getTestHash() . '_Test_table';
        $destinationTableName = $this->getTestHash() . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // create tables
        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                new BigqueryColumn('col1', new Bigquery(
                    Bigquery::TYPE_INT,
                    [],
                )),
                new BigqueryColumn('col2', new Bigquery(
                    Bigquery::TYPE_BIGINT,
                    [],
                )),
                new BigqueryColumn($sourceExtraColumn, new Bigquery(
                    Bigquery::TYPE_INT,
                    [],
                )),
            ]),
            ['col1'],
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            [], //<-- dont create primary keys allow duplicates
        );
        $bqClient->runQuery($bqClient->query($sql));
        $insert = [];
        foreach ([['1', '1', '3'], ['2', '2', '2'], ['2', '2', '2'], ['3', '2', '3'], ['4', '4', '4']] as $i) {
            $insert[] = sprintf('(%s)', implode(',', $i));
        }

        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insert),
        )));

        // create tables
        if ($isTypedTable) {
            $tableDestDef = $this->createDestinationTypedTable($bucketDatabaseName, $destinationTableName, $bqClient);
        } else {
            $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $bqClient);
        }

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
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('col2');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName($sourceExtraColumn)
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

        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'col1';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setImportStrategy($isTypedTable ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp')
                ->setCreateMode(ImportOptions\CreateMode::REPLACE), // <- just prove that this has no effect on import
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        // 1 row unique from source, 3 rows deduped from source and destination
        $this->assertSame(4, $ref->getRowsCount());
        $this->assertTimestamp($bqClient, $bucketDatabaseName, $destinationTableName);
        $data = $this->fetchTable(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            ['col1', 'col3'],
        );
        $this->assertEqualsCanonicalizing([
            [
                'col1' => '1',
                'col3' => '3',
            ],
            [
                'col1' => '2',
                'col3' => '2',
            ],
            [
                'col1' => '3',
                'col3' => '3',
            ],
            [
                'col1' => '4',
                'col3' => '4',
            ],
        ], $data);
    }


    public function importTableFromTableFullLoadWithTimestampTableWithTypesProvider(): Generator
    {
        yield 'no feature' => [
            'features' => [],
            'nOfUpdatedTimestamps' => 2,
        ];
    }

    /**
     * @dataProvider importTableFromTableFullLoadWithTimestampTableWithTypesProvider
     * @param string[] $features
     * Full load to storage
     * timestamp is updated on different features
     */
    public function testImportTableFromTableFullLoadWithTimestampTableWithTypes(
        array $features,
        int $nOfUpdatedTimestamps,
    ): void {
        $sourceTableName = $this->getTestHash() . '_Test_table';
        $destinationTableName = $this->getTestHash() . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // create tables
        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
            ]),
            ['col1'],
        );
        $this->createTableFromDefinition($this->projectCredentials, $tableSourceDef);

        $bqClient->runQuery($bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`col1`, `col2`) VALUES
('1', 'change'),
('3', 'test3'),
('4', 'test4')
SQL,
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
        )));

        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            ['col1'],
        );
        $this->createTableFromDefinition($this->projectCredentials, $tableDestDef);

        $bqClient->runQuery($bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`col1`, `col2`, `_timestamp`) VALUES
('1', 'test', '2021-01-01 00:00:00'),
('2', 'test2', '2021-01-01 00:00:00'),
('3', 'test3', '2021-01-01 00:00:00')
SQL,
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));

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
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('col2');
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
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setImportStrategy(ImportOptions\ImportStrategy::USER_DEFINED_TABLE)
                ->setTimestampColumn('_timestamp'),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            $features,
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertSame(3, $response->getImportedRowsCount());

        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(4, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // assert updated timestamps
        $timestamps = iterator_to_array($bqClient->runQuery($bqClient->query(sprintf(
            'SELECT COUNT(*) FROM %s.%s WHERE `_timestamp` <> \'2021-01-01 00:00:00\'',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )))->getIterator());
        $timestamps = $timestamps[0];
        $this->assertIsArray($timestamps);
        $this->assertSame($nOfUpdatedTimestamps, array_values($timestamps)[0]);

        // cleanup
        $bqClient->runQuery($bqClient->query(
            (new BigqueryTableQueryBuilder())->getDropTableCommand(
                $tableSourceDef->getSchemaName(),
                $tableSourceDef->getTableName(),
            ),
        ));
        $bqClient->runQuery($bqClient->query(
            (new BigqueryTableQueryBuilder())->getDropTableCommand(
                $tableDestDef->getSchemaName(),
                $tableDestDef->getTableName(),
            ),
        ));
    }

    /**
     * Test incremental load with nullable property mismatch
     * Source has nullable column, destination has non-nullable column
     */
    public function testIncrementalLoadNullableMismatch(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source with nullable column
        $sourceTableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                new BigqueryColumn('col1', new Bigquery(Bigquery::TYPE_STRING, ['nullable' => true])),
                BigqueryColumn::createGenericColumn('col2'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $sourceTableDef->getSchemaName(),
            $sourceTableDef->getTableName(),
            $sourceTableDef->getColumnsDefinitions(),
            $sourceTableDef->getPrimaryKeysNames(),
        )));
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES (%s, %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            BigqueryQuote::quote('value1'),
            BigqueryQuote::quote('value2'),
        )));

        // Create destination with non-nullable column
        $destTableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                new BigqueryColumn('col1', new Bigquery(Bigquery::TYPE_STRING, ['nullable' => false])),
                BigqueryColumn::createGenericColumn('col2'),
            ]),
            [],
        );
        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $destTableDef->getSchemaName(),
            $destTableDef->getTableName(),
            $destTableDef->getColumnsDefinitions(),
            $destTableDef->getPrimaryKeysNames(),
        )));

        // Try incremental load - should fail due to nullable mismatch
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

        $this->expectException(DriverColumnsMismatchException::class);
        $this->expectExceptionMessageMatches('/Columns .* do not match/');

        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    /**
     * Test incremental load when destination has columns that source doesn't have
     * This should fail - all destination columns must exist in source
     */
    public function testIncrementalLoadMissingColumnsInSource(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source with only 2 columns
        $sourceTableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $sourceTableDef->getSchemaName(),
            $sourceTableDef->getTableName(),
            $sourceTableDef->getColumnsDefinitions(),
            $sourceTableDef->getPrimaryKeysNames(),
        )));
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES (%s, %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('2'),
        )));

        // Create destination with 3 columns (has col3 that source doesn't have)
        $destTableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createGenericColumn('col3'),
            ]),
            [],
        );
        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $destTableDef->getSchemaName(),
            $destTableDef->getTableName(),
            $destTableDef->getColumnsDefinitions(),
            $destTableDef->getPrimaryKeysNames(),
        )));

        // Map all destination columns
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('col2');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col3')
            ->setDestinationColumnName('col3');

        // Try incremental load - should fail because source doesn't have col3
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
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
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $this->expectException(DriverColumnsMismatchException::class);

        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    /**
     * Test incremental load when source has extra columns not in destination
     * Extra columns in source should be ignored (only mapped columns are loaded)
     */
    public function testIncrementalLoadExtraColumnsInSource(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source with 4 columns
        $sourceTableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createGenericColumn('col3'),
                BigqueryColumn::createGenericColumn('col4'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $sourceTableDef->getSchemaName(),
            $sourceTableDef->getTableName(),
            $sourceTableDef->getColumnsDefinitions(),
            $sourceTableDef->getPrimaryKeysNames(),
        )));
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES (%s, %s, %s, %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('2'),
            BigqueryQuote::quote('3'),
            BigqueryQuote::quote('4'),
        )));

        // Create destination with only 2 columns
        $destTableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col3'),
            ]),
            [],
        );
        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $destTableDef->getSchemaName(),
            $destTableDef->getTableName(),
            $destTableDef->getColumnsDefinitions(),
            $destTableDef->getPrimaryKeysNames(),
        )));

        // Map only destination columns (col1, col3)
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

        // Incremental load - should succeed, extra columns (col2, col4) ignored
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
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

        // Verify only mapped columns were imported
        $this->assertSame(1, $response->getImportedRowsCount());
        $importedColumns = iterator_to_array($response->getImportedColumns());
        $this->assertCount(2, $importedColumns);
        $this->assertContains('col1', $importedColumns);
        $this->assertContains('col3', $importedColumns);

        // Verify data in destination
        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
        $rows = iterator_to_array($result);
        $this->assertCount(1, $rows);
        $this->assertSame('1', $rows[0]['col1']);
        $this->assertSame('3', $rows[0]['col3']);
    }

    /**
     * Test UPDATE_DUPLICATES with empty dedupColumnsNames (should use primary keys)
     */
    public function testUpdateDuplicatesWithEmptyDedupColumns(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create tables with primary key
        $this->createDestinationTypedTable($bucketDatabaseName, $destinationTableName, $bqClient);
        $sourceTableDef = $this->createDestinationTypedTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // Import with UPDATE_DUPLICATES but no dedupColumnsNames
        // Should use table's primary keys for deduplication
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
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setTimestampColumn('_timestamp')
                // No dedupColumnsNames - should use primary keys if available
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Should succeed with deduplication
        $this->assertSame(3, $response->getImportedRowsCount());
    }

    /**
     * Test deduplication with multiple dedup columns (composite key)
     */
    public function testDeduplicationWithMultipleColumns(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source table with duplicates on composite key (col1 + col2)
        $sourceTableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createGenericColumn('col3'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $sourceTableDef->getSchemaName(),
            $sourceTableDef->getTableName(),
            $sourceTableDef->getColumnsDefinitions(),
            $sourceTableDef->getPrimaryKeysNames(),
        )));

        // Insert rows with duplicates: (1,1), (1,2), (2,1), (1,1) again
        $rows = [
            ['1', '1', 'a'],
            ['1', '2', 'b'],
            ['2', '1', 'c'],
            ['1', '1', 'd'], // Duplicate of first row on col1+col2
        ];
        foreach ($rows as $row) {
            $bqClient->runQuery($bqClient->query(sprintf(
                'INSERT INTO %s.%s (col1, col2, col3, _timestamp) VALUES (%s, %s, %s, CURRENT_TIMESTAMP())',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($sourceTableName),
                BigqueryQuote::quote($row[0]),
                BigqueryQuote::quote($row[1]),
                BigqueryQuote::quote($row[2]),
            )));
        }

        // Create destination
        $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $bqClient);

        // Import with dedup on col1 AND col2 (composite key)
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

        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'col1';
        $dedupColumns[] = 'col2';

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns)
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

        // Should have 3 unique rows (1,1), (1,2), (2,1) - the duplicate (1,1) is deduplicated
        $this->assertSame(3, $response->getImportedRowsCount());

        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT col1, col2, col3 FROM %s.%s ORDER BY col1, col2',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
        $rows = iterator_to_array($result);
        $this->assertCount(3, $rows);
        // First occurrence of (1,1) should be kept with updated timestamp
        $this->assertSame('1', $rows[0]['col1']);
        $this->assertSame('1', $rows[0]['col2']);
    }

    /**
     * Test INSERT_DUPLICATES with dedupColumnsNames (should be ignored)
     */
    public function testInsertDuplicatesIgnoresDedupColumns(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        // Create source with duplicates
        $this->createDestinationTable($bucketDatabaseName, $sourceTableName, $bqClient);
        // Add duplicate row (same col1 value)
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (col1, col2, col3, _timestamp) VALUES (%s, %s, %s, CURRENT_TIMESTAMP())',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            BigqueryQuote::quote('1'), // Duplicate col1
            BigqueryQuote::quote('different'),
            BigqueryQuote::quote('different'),
        )));

        // Create destination
        $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $bqClient);

        // Import with INSERT_DUPLICATES (dedup columns should be ignored)
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

        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'col1';

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns), // Should be ignored
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Should insert ALL rows including duplicates (4 total: 3 original + 1 duplicate)
        $this->assertSame(4, $response->getImportedRowsCount());

        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT COUNT(*) as cnt FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
        $row = iterator_to_array($result)[0];
        $this->assertSame(4, (int) $row['cnt']);
    }
}

