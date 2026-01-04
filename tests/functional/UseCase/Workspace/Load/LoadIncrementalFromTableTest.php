<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace\Load;

use Generator;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Cloud\Core\Exception\NotFoundException;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\ColumnsMismatchException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\Import\ImportValidationException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Throwable;

class LoadIncrementalFromTableTest extends BaseImportTestCase
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

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);

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

        $pkColumns = ['col1'];
        $dedupColumns = new RepeatedField(GPBType::STRING);

        foreach ($pkColumns as $pkColumn) {
            $dedupColumns[] = $pkColumn;
        }

        // create tables
        if ($isTypedTable) {
            $this->createDestinationTypedTable($bucketDatabaseName, $destinationTableName, $bqClient, $pkColumns);
        } else {
            $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $bqClient, $pkColumns);
        }

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
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName($sourceExtraColumn)
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
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setImportStrategy($isTypedTable ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp')
                ->setCreateMode(ImportOptions\CreateMode::REPLACE), // <- just prove that this has no effect on import
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);

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
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setImportStrategy(ImportOptions\ImportStrategy::USER_DEFINED_TABLE)
                ->setTimestampColumn('_timestamp'),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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
    }

    public function testIncrementalImportWithWhereFilterAndDeduplication(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_filter_dedup';
        $destinationTableName = $this->getTestHash() . '_dest_filter_dedup';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $pkColumns = ['id'];
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);

        $qb = new BigqueryTableQueryBuilder();

        // Source table with status column for filtering
        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('name'),
                BigqueryColumn::createGenericColumn('status'),
            ]),
            [],
        );

        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            [],
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Insert source data (NO duplicates in source - all unique ids)
        $insert = [];
        foreach ([
                     ['1', 'active1', 'active'],
                     ['2', 'active2', 'active'],
                     ['3', 'inactive3', 'inactive'],
                     ['4', 'active4', 'active'],
                     ['5', 'pending5', 'pending'],
                 ] as $row) {
            $quotedValues = array_map([BigqueryQuote::class, 'quote'], $row);
            $insert[] = sprintf('(%s)', implode(',', $quotedValues));
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insert),
        )));

        // Destination table
        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('name'),
                BigqueryColumn::createGenericColumn('status'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            $pkColumns,
        );

        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $pkColumns,
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Insert initial destination data
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (id, name, status, `_timestamp`) VALUES ' .
            '(%s, %s, %s, TIMESTAMP %s), ' .
            '(%s, %s, %s, TIMESTAMP %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('old_name'),
            BigqueryQuote::quote('active'),
            BigqueryQuote::quote('2020-01-01 00:00:00'),
            BigqueryQuote::quote('6'),
            BigqueryQuote::quote('keep_inactive'),
            BigqueryQuote::quote('inactive'),
            BigqueryQuote::quote('2020-01-01 00:00:00'),
        )));

        // Prepare import command
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        // Column mappings (1:1, no renaming)
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'name', 'status'] as $column) {
            $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($column)
                ->setDestinationColumnName($column);
        }

        // WHERE filter: status = 'active'
        $whereFilters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $values = new RepeatedField(GPBType::STRING);
        $values[] = 'active';
        $whereFilters[] = (new TableWhereFilter())
            ->setColumnsName('status')
            ->setOperator(Operator::eq)
            ->setValues($values)
            ->setDataType(DataType::STRING);

        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
                ->setColumnMappings($columnMappings)
                ->setWhereFilters($whereFilters),
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
                ->setDedupColumnsNames($this->buildDedupColumns($pkColumns))
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Assert import counts - only 'active' status rows (3 rows)
        $this->assertSame(3, $response->getImportedRowsCount(), 'Should import 3 rows with status=active');

        // CRITICAL: Verify SQL path was used (NOT COPY optimization)
        // Filters force SelectSource usage, which prevents COPY optimization
        $this->assertSame(
            ['id', 'name', 'status'],
            iterator_to_array($response->getImportedColumns()),
            'SQL path should return imported column names (filters prevent COPY optimization)',
        );

        // Verify final row count after filter and merge
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(4, $ref->getRowsCount(), 'Final table: 3 active (1,2,4) + 1 kept (6)');
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // Verify timestamp was updated
        $this->assertTimestamp($bqClient, $bucketDatabaseName, $destinationTableName);

        // Verify data correctness - only active rows imported
        $data = $this->fetchTable(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            ['id', 'name', 'status'],
        );

        $this->assertEqualsCanonicalizing([
            ['id' => '1', 'name' => 'active1', 'status' => 'active'],
            ['id' => '2', 'name' => 'active2', 'status' => 'active'],
            ['id' => '4', 'name' => 'active4', 'status' => 'active'],
            ['id' => '6', 'name' => 'keep_inactive', 'status' => 'inactive'],
        ], $data, 'Should import only filtered rows and preserve existing');

        // Additional assertion: verify filtering worked
        foreach ($data as $row) {
            if ($row['id'] !== '6') {
                $this->assertSame('active', $row['status'], 'Only active status should be imported from source');
            }
        }

        // Cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $sourceTableName),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName),
            ));
        } catch (NotFoundException $e) {
            // OK
        }
    }

    public function testIncrementalImportWithDedupFailsWhenColumnNamesDoNotMatch(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_mapping_dedup';
        $destinationTableName = $this->getTestHash() . '_dest_mapping_dedup';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);

        $qb = new BigqueryTableQueryBuilder();

        // Source table: columns col1, col2, col3
        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createGenericColumn('col3'),
            ]),
            [],
        );

        // Cleanup and create source table
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            [],
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Insert source data (NO duplicates - source has unique col1 values)
        $insert = [];
        foreach ([
                     ['1', 'value1', 'data1'],
                     ['2', 'value2', 'data2'],
                     ['3', 'value3', 'data3'],
                     ['4', 'value4', 'data4'],
                 ] as $row) {
            $quotedValues = array_map([BigqueryQuote::class, 'quote'], $row);
            $insert[] = sprintf('(%s)', implode(',', $quotedValues));
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insert),
        )));

        // Destination table: columns id, name, description, _timestamp
        // Different column names - requires mapping
        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('name'),
                BigqueryColumn::createGenericColumn('description'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            [],
        );

        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            [],
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Insert initial destination data
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (id, name, description, `_timestamp`) VALUES ' .
            '(%s, %s, %s, TIMESTAMP %s), ' .
            '(%s, %s, %s, TIMESTAMP %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('initial_name'),
            BigqueryQuote::quote('initial_desc'),
            BigqueryQuote::quote('2020-01-01 00:00:00'),
            BigqueryQuote::quote('5'),
            BigqueryQuote::quote('keep_this'),
            BigqueryQuote::quote('keep_desc'),
            BigqueryQuote::quote('2020-01-01 00:00:00'),
        )));

        // Prepare import command with column mapping
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        // Column mappings: col1→id, col2→name, col3→description
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('id');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('name');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col3')
            ->setDestinationColumnName('description');

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

        // Incremental import with UPDATE_DUPLICATES and dedup on 'id' column
        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'id';

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        // Expect exception when trying incremental import with column renaming
        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('Import should fail when column names do not match for incremental UPDATE_DUPLICATES');
        } catch (ImportValidationException $e) {
            // Expected - column renaming not supported for incremental with dedup
            $this->assertStringContainsString('col1', $e->getMessage(), 'Error should mention source column');
            $this->assertStringContainsString('id', $e->getMessage(), 'Error should mention destination column');
        }

        // Cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $sourceTableName),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName),
            ));
        } catch (NotFoundException $e) {
            // OK
        }
    }

    /**
     * @dataProvider typedTablesProvider
     */
    public function testIncrementalImportEmptySourceWithDedupColumns(bool $isTypedTable): void
    {
        $sourceTableName = $this->getTestHash() . '_src_empty_dedup';
        $destinationTableName = $this->getTestHash() . '_dest_empty_dedup';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $qb = new BigqueryTableQueryBuilder();

        // Create empty source table
        if ($isTypedTable) {
            $tableSourceDef = new BigqueryTableDefinition(
                $bucketDatabaseName,
                $sourceTableName,
                false,
                new ColumnCollection([
                    new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_INT, [])),
                    new BigqueryColumn('name', new Bigquery(Bigquery::TYPE_STRING, [])),
                ]),
                [],
            );
        } else {
            $tableSourceDef = new BigqueryTableDefinition(
                $bucketDatabaseName,
                $sourceTableName,
                false,
                new ColumnCollection([
                    BigqueryColumn::createGenericColumn('id'),
                    BigqueryColumn::createGenericColumn('name'),
                ]),
                [],
            );
        }

        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            [],
        );
        $bqClient->runQuery($bqClient->query($sql));

        // DO NOT insert any data - keep source empty
        $sourceRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->assertSame(0, $sourceRef->getRowsCount(), 'Source table should be empty');

        // Create destination table with initial data
        if ($isTypedTable) {
            $tableDestDef = new BigqueryTableDefinition(
                $bucketDatabaseName,
                $destinationTableName,
                false,
                new ColumnCollection([
                    new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_INT, [])),
                    new BigqueryColumn('name', new Bigquery(Bigquery::TYPE_STRING, [])),
                    BigqueryColumn::createTimestampColumn('_timestamp'),
                ]),
                [],
            );
        } else {
            $tableDestDef = new BigqueryTableDefinition(
                $bucketDatabaseName,
                $destinationTableName,
                false,
                new ColumnCollection([
                    BigqueryColumn::createGenericColumn('id'),
                    BigqueryColumn::createGenericColumn('name'),
                    BigqueryColumn::createTimestampColumn('_timestamp'),
                ]),
                [],
            );
        }

        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            [],
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Insert initial data into destination
        if ($isTypedTable) {
            $bqClient->runQuery($bqClient->query(sprintf(
                'INSERT INTO %s.%s (id, name, `_timestamp`) VALUES ' .
                '(1, %s, TIMESTAMP %s), ' .
                '(2, %s, TIMESTAMP %s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($destinationTableName),
                BigqueryQuote::quote('Alice'),
                BigqueryQuote::quote('2020-01-01 00:00:00'),
                BigqueryQuote::quote('Bob'),
                BigqueryQuote::quote('2020-01-01 00:00:00'),
            )));
        } else {
            $bqClient->runQuery($bqClient->query(sprintf(
                'INSERT INTO %s.%s (id, name, `_timestamp`) VALUES ' .
                '(%s, %s, TIMESTAMP %s), ' .
                '(%s, %s, TIMESTAMP %s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($destinationTableName),
                BigqueryQuote::quote('1'),
                BigqueryQuote::quote('Alice'),
                BigqueryQuote::quote('2020-01-01 00:00:00'),
                BigqueryQuote::quote('2'),
                BigqueryQuote::quote('Bob'),
                BigqueryQuote::quote('2020-01-01 00:00:00'),
            )));
        }

        $destRefBefore = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $initialRowCount = $destRefBefore->getRowsCount();
        $this->assertSame(2, $initialRowCount, 'Destination should have 2 initial rows');

        // Prepare import command
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('id')
            ->setDestinationColumnName('id');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('name')
            ->setDestinationColumnName('name');

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

        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'id';

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setImportStrategy($isTypedTable ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp'),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        // Execute import - should succeed even with empty source
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify import succeeded with zero rows imported
        $this->assertSame(0, $response->getImportedRowsCount(), 'Should import 0 rows from empty source');

        // Verify destination table unchanged
        $destRefAfter = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(
            $initialRowCount,
            $destRefAfter->getRowsCount(),
            'Destination row count should be unchanged',
        );
        $this->assertSame(
            $initialRowCount,
            $response->getTableRowsCount(),
            'Response should reflect unchanged row count',
        );

        // Verify destination data unchanged
        $data = $this->fetchTable(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            ['id', 'name'],
        );

        $this->assertCount(2, $data, 'Should still have 2 rows');
        if ($isTypedTable) {
            $this->assertEqualsCanonicalizing([
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ], $data, 'Original data should be preserved');
        } else {
            $this->assertEqualsCanonicalizing([
                ['id' => '1', 'name' => 'Alice'],
                ['id' => '2', 'name' => 'Bob'],
            ], $data, 'Original data should be preserved');
        }

        // Cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $sourceTableName),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName),
            ));
        } catch (NotFoundException $e) {
            // OK
        }
    }

    /**
     * @dataProvider typedTablesProvider
     */
    public function testIncrementalImportWithNullInDedupColumn(bool $isTypedTable): void
    {
        $sourceTableName = $this->getTestHash() . '_src_null_dedup';
        $destinationTableName = $this->getTestHash() . '_dest_null_dedup';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $qb = new BigqueryTableQueryBuilder();

        // Create source table with nullable columns
        if ($isTypedTable) {
            $tableSourceDef = new BigqueryTableDefinition(
                $bucketDatabaseName,
                $sourceTableName,
                false,
                new ColumnCollection([
                    new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_STRING, ['nullable' => true])),
                    new BigqueryColumn('name', new Bigquery(Bigquery::TYPE_STRING, [])),
                ]),
                [],
            );
        } else {
            $tableSourceDef = new BigqueryTableDefinition(
                $bucketDatabaseName,
                $sourceTableName,
                false,
                new ColumnCollection([
                    BigqueryColumn::createGenericColumn('id'),
                    BigqueryColumn::createGenericColumn('name'),
                ]),
                [],
            );
        }

        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            [],
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Try to insert data with NULL in id column (dedup column)
        // For string tables, BigQuery may reject NULL during INSERT
        try {
            $bqClient->runQuery($bqClient->query(sprintf(
                'INSERT INTO %s.%s (id, name) VALUES (%s, %s), (NULL, %s), (%s, %s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($sourceTableName),
                BigqueryQuote::quote('1'),
                BigqueryQuote::quote('Alice'),
                BigqueryQuote::quote('Bob'),
                BigqueryQuote::quote('3'),
                BigqueryQuote::quote('Charlie'),
            )));
        } catch (BadRequestException $e) {
            // For string tables, BigQuery rejects NULL during INSERT
            // This documents that NULL values cannot even be inserted into source tables
            $this->assertStringContainsString('null', strtolower($e->getMessage()));
            // Skip the rest of the test - we've documented the behavior
            try {
                $bqClient->runQuery($bqClient->query(
                    $qb->getDropTableCommand($bucketDatabaseName, $sourceTableName),
                ));
            } catch (NotFoundException $ex) {
                // OK
            }
            return;
        }

        // Create destination table
        if ($isTypedTable) {
            $tableDestDef = new BigqueryTableDefinition(
                $bucketDatabaseName,
                $destinationTableName,
                false,
                new ColumnCollection([
                    // explicitly set by
                    new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_STRING, ['nullable' => false])),
                    new BigqueryColumn('name', new Bigquery(Bigquery::TYPE_STRING, [])),
                    BigqueryColumn::createTimestampColumn('_timestamp'),
                ]),
                ['id'],
            );
        } else {
            $tableDestDef = new BigqueryTableDefinition(
                $bucketDatabaseName,
                $destinationTableName,
                false,
                new ColumnCollection([
                    // by default, all generic columns are nullable = false
                    BigqueryColumn::createGenericColumn('id'),
                    BigqueryColumn::createGenericColumn('name'),
                    BigqueryColumn::createTimestampColumn('_timestamp'),
                ]),
                ['id'],
            );
        }

        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            ['id'],
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Prepare import command
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('id')
            ->setDestinationColumnName('id');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('name')
            ->setDestinationColumnName('name');

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

        // Set dedup columns to 'id' which contains NULL
        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'id';

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setImportStrategy($isTypedTable ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp'),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        // The system should reject NULL values in dedup columns
        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('Import should fail when dedup column contains NULL values');
        } catch (ImportValidationException $e) {
            // Expected for typed tables - validation catches NULL
            $this->assertStringContainsString('null', strtolower($e->getMessage()));
            $this->assertStringContainsString('id', strtolower($e->getMessage()));
        } catch (BadRequestException $e) {
            // Expected for string tables - BigQuery catches NULL during query execution
            $this->assertStringContainsString('null', strtolower($e->getMessage()));
            $this->assertStringContainsString('id', strtolower($e->getMessage()));
        } catch (Throwable $e) {
            if ($isTypedTable) {
                $this->fail(sprintf(
                    'Typed table should throw ImportValidationException, got %s: %s',
                    get_class($e),
                    $e->getMessage(),
                ));
            }
            // Catch any other exception and verify it mentions NULL
            $this->assertStringContainsString(
                'null',
                strtolower($e->getMessage()),
                'Error should mention NULL. Got: ' . $e->getMessage(),
            );
        }

        // Cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $sourceTableName),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName),
            ));
        } catch (NotFoundException $e) {
            // OK
        }
    }

    public function testIncrementalFailsWhenDestinationMissingColumns(): void
    {
        $sourceTableName = $this->getTestHash() . '_copy_src_missing_dest';
        $destinationTableName = $this->getTestHash() . '_copy_dest_missing_dest';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);

        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('name'),
                new BigqueryColumn('_timestamp', new Bigquery(Bigquery::TYPE_TIMESTAMP, ['nullable' => false])),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();

        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        )));

        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (id, name, `_timestamp`) VALUES (%s, %s, CURRENT_TIMESTAMP())',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('alpha'),
        )));

        $cmdFull = $this->createImportCommand(
            $bucketDatabaseName,
            $sourceTableName,
            $destinationTableName,
            ['id', 'name', '_timestamp'],
            ImportOptions\ImportType::FULL,
            ImportOptions\DedupType::INSERT_DUPLICATES,
            [],
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $handler(
            $this->projectCredentials,
            $cmdFull,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $bqClient->runQuery($bqClient->query(sprintf(
            'ALTER TABLE %s.%s ADD COLUMN %s STRING',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            BigqueryQuote::quoteSingleIdentifier('test'),
        )));

        $cmdIncremental = $this->createImportCommand(
            $bucketDatabaseName,
            $sourceTableName,
            $destinationTableName,
            ['id', 'name', 'test', '_timestamp'],
            ImportOptions\ImportType::INCREMENTAL,
            ImportOptions\DedupType::UPDATE_DUPLICATES,
            ['id'],
        );

        try {
            $handler(
                $this->projectCredentials,
                $cmdIncremental,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('Incremental load should fail when destination is missing mapped columns.');
        } catch (ColumnsMismatchException $e) {
            $this->assertStringContainsString('columns are missing in workspace table', $e->getMessage());
            $this->assertStringContainsString('test', $e->getMessage());
        }
    }

    public function testIncrementalFailsWhenSourceMissingColumns(): void
    {
        $sourceTableName = $this->getTestHash() . '_copy_src_missing_source';
        $destinationTableName = $this->getTestHash() . '_copy_dest_missing_source';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);

        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('name'),
                new BigqueryColumn('_timestamp', new Bigquery(Bigquery::TYPE_TIMESTAMP, ['nullable' => false])),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();

        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        )));

        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (id, name, `_timestamp`) VALUES (%s, %s, CURRENT_TIMESTAMP())',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('alpha'),
        )));

        $cmdFull = $this->createImportCommand(
            $bucketDatabaseName,
            $sourceTableName,
            $destinationTableName,
            ['id', 'name', '_timestamp'],
            ImportOptions\ImportType::FULL,
            ImportOptions\DedupType::INSERT_DUPLICATES,
            [],
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $handler(
            $this->projectCredentials,
            $cmdFull,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $bqClient->runQuery($bqClient->query(sprintf(
            'ALTER TABLE %s.%s DROP COLUMN %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            BigqueryQuote::quoteSingleIdentifier('name'),
        )));

        $cmdIncremental = $this->createImportCommand(
            $bucketDatabaseName,
            $sourceTableName,
            $destinationTableName,
            ['id', '_timestamp'],
            ImportOptions\ImportType::INCREMENTAL,
            ImportOptions\DedupType::UPDATE_DUPLICATES,
            ['id'],
        );

        try {
            $handler(
                $this->projectCredentials,
                $cmdIncremental,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('Incremental load should fail when source is missing mapped columns.');
        } catch (ColumnsMismatchException $e) {
            $this->assertStringContainsString('columns are missing in source table', $e->getMessage());
            $this->assertStringContainsString('name', $e->getMessage());
        }
    }

    /**
     * Helper method to create a LoadTableToWorkspaceCommand with standard configuration.
     *
     * @param array<string> $columns
     * @param array<string> $dedupColumns
     */
    private function createImportCommand(
        string $schema,
        string $sourceTable,
        string $destinationTable,
        array $columns,
        int $importType,
        int $dedupType,
        array $dedupColumns,
    ): LoadTableToWorkspaceCommand {
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $schema;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach ($columns as $column) {
            $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($column)
                ->setDestinationColumnName($column);
        }
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTable)
                ->setColumnMappings($columnMappings),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTable),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE)
                ->setImportType($importType)
                ->setDedupType($dedupType)
                ->setDedupColumnsNames(ProtobufHelper::arrayToRepeatedString($dedupColumns))
                ->setTimestampColumn('_timestamp')
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setImportAsNull(new RepeatedField(GPBType::STRING)),
        );

        return $cmd;
    }
}
