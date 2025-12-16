<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace\Load;

use Generator;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

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

        // create tables
        if ($isTypedTable) {
            $tableDestDef = $this->createDestinationTypedTable($bucketDatabaseName, $destinationTableName, $bqClient);
        } else {
            $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $bqClient);
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
}
