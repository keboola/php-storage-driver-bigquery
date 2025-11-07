<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\FromTable;

use Generator;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\Backend\BigQuery\Clustering;
use Keboola\StorageDriver\Backend\BigQuery\RangePartitioning;
use Keboola\StorageDriver\BigQuery\Handler\Table\BadExportFilterParametersException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ColumnsMismatchException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\MaximumLengthOverflowException;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\Import\ImportValidationException;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use LogicException;
use Throwable;

/**
 * @group Import
 */
class ImportTableFromTableTest extends BaseImportTestCase
{
    /**
     * Full load to workspace simulation
     * This is input mapping, no timestamp is updated
     */
    public function testImportTableFromTableFullLoadNoDedup(): void
    {
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
                BigqueryColumn::createGenericColumn('col3'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));
        $insert = [];
        foreach ([['1', '1', '1'], ['2', '2', '2'], ['3', '3', '3']] as $i) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = BigqueryQuote::quote($item);
            }
            $insert[] = sprintf('(%s)', implode(',', $quotedValues));
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insert),
        )));

        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col4'), // <- different col rename
            ]),
            [],
        );
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));

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
            ->setDestinationColumnName('col4');
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
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE), // <- just prove that this has no effect on import
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertSame(3, $response->getImportedRowsCount());
        $this->assertSame(
            [], // optimized full load is not returning imported columns
            iterator_to_array($response->getImportedColumns()),
        );
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // cleanup
        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
        ));
        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
        ));
    }

    /**
     * Full load to storage from workspace
     * This is output mapping, timestamp is updated
     */
    public function testImportTableFromTableFullLoadWithTimestamp(): void
    {
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
                BigqueryColumn::createGenericColumn('col3'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));
        $insert = [];
        foreach ([['1', '1', '1'], ['2', '2', '2'], ['3', '3', '3']] as $i) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = BigqueryQuote::quote($item);
            }
            $insert[] = sprintf('(%s)', implode(',', $quotedValues));
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insert),
        )));

        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col4'), // <- different col rename
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            [],
        );
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));

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
            ->setDestinationColumnName('col4');
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
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp'),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertSame(3, $response->getImportedRowsCount());
        $this->assertSame(
            [
                'col1',
                'col4',
            ],
            iterator_to_array($response->getImportedColumns()),
        );
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        $this->assertTimestamp($bqClient, $bucketDatabaseName, $destinationTableName);

        // cleanup
        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
        ));
        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
        ));
    }

    public function testImportTableFromTableWithoutTimestampInMapping(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_no_ts';
        $destinationTableName = $this->getTestHash() . '_dest_no_ts';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        $sourceDefinition = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $sourceDefinition->getSchemaName(),
            $sourceDefinition->getTableName(),
            $sourceDefinition->getColumnsDefinitions(),
            $sourceDefinition->getPrimaryKeysNames(),
        )));

        $rows = [
            ['1', 'first'],
            ['2', 'second'],
            ['3', 'third'],
        ];
        $insertValues = [];
        foreach ($rows as $index => [$col1, $col2]) {
            $insertValues[] = sprintf(
                '(%s, %s, TIMESTAMP %s)',
                BigqueryQuote::quote($col1),
                BigqueryQuote::quote($col2),
                BigqueryQuote::quote(sprintf('2023-01-0%d 00:00:00', $index + 1)),
            );
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (col1, col2, `_timestamp`) VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insertValues),
        )));

        $destinationDefinition = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
            ]),
            [],
        );
        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $destinationDefinition->getSchemaName(),
            $destinationDefinition->getTableName(),
            $destinationDefinition->getColumnsDefinitions(),
            $destinationDefinition->getPrimaryKeysNames(),
        )));

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

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd = (new TableImportFromTableCommand())
            ->setSource(
                (new TableImportFromTableCommand\SourceTableMapping())
                    ->setPath($path)
                    ->setTableName($sourceTableName)
                    ->setColumnMappings($columnMappings),
            )
            ->setDestination(
                (new Table())
                    ->setPath($path)
                    ->setTableName($destinationTableName),
            )
            ->setImportOptions(
                (new ImportOptions())
                    ->setImportType(ImportOptions\ImportType::FULL)
                    ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                    ->setImportStrategy(ImportOptions\ImportStrategy::USER_DEFINED_TABLE)
                    ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                    ->setNumberOfIgnoredLines(0),
            );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertSame(3, $response->getImportedRowsCount());
        $this->assertSame(
            ['col1', 'col2'],
            iterator_to_array($response->getImportedColumns()),
        );
        $destinationRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $destinationRef->getRowsCount());

        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($sourceDefinition->getSchemaName(), $sourceDefinition->getTableName()),
        ));
        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($destinationDefinition->getSchemaName(), $destinationDefinition->getTableName()),
        ));
    }

    public function testImportTableFromTableWithFiltersAndLimit(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_filters';
        $destinationTableName = $this->getTestHash() . '_dest_filters';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        $sourceDefinition = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $sourceDefinition->getSchemaName(),
            $sourceDefinition->getTableName(),
            $sourceDefinition->getColumnsDefinitions(),
            $sourceDefinition->getPrimaryKeysNames(),
        )));

        $rows = [
            ['1', 'keep', '2020-01-01 00:00:00'],
            ['2', 'keep', '2100-01-01 00:00:00'],
            ['3', 'drop', '2100-01-01 00:00:00'],
            ['4', 'keep', '2100-02-01 00:00:00'],
        ];
        $insertValues = [];
        foreach ($rows as [$col1, $col2, $timestamp]) {
            $insertValues[] = sprintf(
                '(%s, %s, TIMESTAMP %s)',
                BigqueryQuote::quote($col1),
                BigqueryQuote::quote($col2),
                BigqueryQuote::quote($timestamp),
            );
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (col1, col2, `_timestamp`) VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insertValues),
        )));

        $destinationDefinition = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            [],
        );
        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $destinationDefinition->getSchemaName(),
            $destinationDefinition->getTableName(),
            $destinationDefinition->getColumnsDefinitions(),
            $destinationDefinition->getPrimaryKeysNames(),
        )));

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

        $whereFilters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $values = new RepeatedField(GPBType::STRING);
        $values[] = 'keep';
        $whereFilters[] = (new TableWhereFilter())
            ->setColumnsName('col2')
            ->setOperator(Operator::eq)
            ->setValues($values)
            ->setDataType(DataType::STRING);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $sourceMapping = (new TableImportFromTableCommand\SourceTableMapping())
            ->setPath($path)
            ->setTableName($sourceTableName)
            ->setColumnMappings($columnMappings)
            ->setWhereFilters($whereFilters)
            ->setLimit(2)
            ->setSeconds(60);

        $cmd = (new TableImportFromTableCommand())
            ->setSource($sourceMapping)
            ->setDestination(
                (new Table())
                    ->setPath($path)
                    ->setTableName($destinationTableName),
            )
            ->setImportOptions(
                (new ImportOptions())
                    ->setImportType(ImportOptions\ImportType::FULL)
                    ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                    ->setImportStrategy(ImportOptions\ImportStrategy::USER_DEFINED_TABLE)
                    ->setNumberOfIgnoredLines(0),
            );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT col1, col2 FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));

        $rowsIterator = iterator_to_array($result->getIterator());
        $this->assertCount(2, $rowsIterator);
        foreach ($rowsIterator as $row) {
            $this->assertSame('keep', $row['col2']);
        }

        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($sourceDefinition->getSchemaName(), $sourceDefinition->getTableName()),
        ));
        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($destinationDefinition->getSchemaName(), $destinationDefinition->getTableName()),
        ));
    }

    /**
     * Full load to storage from workspace with deduplication
     * This is output mapping, timestamp is updated
     */
    public function testImportTableFromTableFullLoadWithTimestampWithDeduplication(): void
    {
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
                BigqueryColumn::createGenericColumn('col3'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));
        $insert = [];
        foreach ([['1', '1', '1'], ['2', '2', '2'], ['3', '3', '3'], ['2', '2', '2'], ['3', '3', '3']] as $i) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = BigqueryQuote::quote($item);
            }
            $insert[] = sprintf('(%s)', implode(',', $quotedValues));
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insert),
        )));
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->assertSame(5, $ref->getRowsCount());

        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col4'), // <- different col rename
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            [],
        );
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));

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
            ->setDestinationColumnName('col4');
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
        $dedupCols = new RepeatedField(GPBType::STRING);
        $dedupCols[] = 'col1';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
                ->setDedupColumnsNames($dedupCols),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertSame(5, $response->getImportedRowsCount());
        $this->assertSame(
            [
                'col1',
                'col4',
            ],
            iterator_to_array($response->getImportedColumns()),
        );
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());
        $this->assertTimestamp($bqClient, $bucketDatabaseName, $destinationTableName);
        $data = $this->fetchTable(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            ['col1', 'col4'],
        );
        $this->assertEqualsCanonicalizing([
            [
                'col1' => '1',
                'col4' => '1',
            ],
            [
                'col1' => '3',
                'col4' => '3',
            ],
            [
                'col1' => '2',
                'col4' => '2',
            ],
        ], $data);

        // cleanup
        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
        ));
        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
        ));
    }

    // simulate output mapping load table to table with requirePartition filter
    public function testPartitionedTableWithRequirePartitionFilter(): void
    {
        $tableName = $this->getTestHash() . '_Test_table';
        $destinationTableName = $this->getTestHash() . '_Test_table_final';
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setNullable(false)
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = (new TableColumnShared)
            ->setName('time')
            ->setType(Bigquery::TYPE_TIMESTAMP)
            ->setNullable(false);
        $any = new Any();
        $any->pack(
            (new CreateTableCommand\BigQueryTableMeta())
                ->setClustering((new Clustering())->setFields(['id']))
                ->setRangePartitioning(
                    (new RangePartitioning())
                        ->setField('id')
                        ->setRange(
                            (new RangePartitioning\Range())
                                ->setStart('0')
                                ->setEnd('10')
                                ->setInterval('1'),
                        ),
                )
                ->setRequirePartitionFilter(true),
        );
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns)
            ->setMeta($any);
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $columns[] = (new TableColumnShared)
            ->setName('_timestamp')
            ->setType(Bigquery::TYPE_TIMESTAMP)
            ->setNullable(false);

        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($destinationTableName)
            ->setColumns($columns)
            ->setMeta($any);
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('id')
            ->setDestinationColumnName('id');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('time')
            ->setDestinationColumnName('time');
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($tableName)
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
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setNumberOfIgnoredLines(0)
                ->setImportStrategy(ImportOptions\ImportStrategy::USER_DEFINED_TABLE)
                ->setTimestampColumn('_timestamp'),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            // 'Cannot query over table 'xxx.xxx' without a filter over column(s) 'id'
            // that can be used for partition elimination'
            $this->assertInstanceOf(BadExportFilterParametersException::class, $e, sprintf(
                'Expected error instance "%s" not thrown. Got "%s" with message "%s"',
                BadExportFilterParametersException::class,
                $e::class,
                $e->getMessage(),
            ));
            $this->assertStringContainsString(
                'without a filter over column(s) \'id\' that can be used for partition elimination',
                $e->getMessage(),
            );
        }
    }

    public function testLoadNullsToRequiredFields(): void
    {
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
                new BigqueryColumn('col3', new Bigquery(
                    Bigquery::TYPE_STRING,
                    [
                        'nullable' => true,
                        'default' => '\'\'',
                    ],
                )),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));
        $insert = [];
        // data in source table have nulls -> should fail, because we are trying to fit null value in to required field
        foreach ([['1', '1', '1'], ['2', '2', '2'], ['3', '3', null]] as $i) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = is_null($item) ? 'null' : BigqueryQuote::quote($item);
            }
            $insert[] = sprintf('(%s)', implode(',', $quotedValues));
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insert),
        )));

        $tableDestDef = new BigqueryTableDefinition(
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
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));

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
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE), // <- just prove that this has no effect on import
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('should fail because of nulls in required field');
        } catch (ImportValidationException $e) {
            $this->assertSame('Required field col3 cannot be null', $e->getMessage());
        } finally {
            // cleanup
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
            ));
        }
    }

    public function importTypeProvide(): Generator
    {
        yield 'full' => [ImportOptions\ImportType::FULL];
        yield 'incremental' => [ImportOptions\ImportType::INCREMENTAL];
    }

    /**
     * @dataProvider importTypeProvide
     */
    public function testLoadDataToDifferentColumnTypeEndsWithMismatchException(int $importType): void
    {
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
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('price'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));
        $insert = [];
        foreach ([['1', 'too expensive'], ['2', 'cheap'], ['3', 'way too expensive']] as $i) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = BigqueryQuote::quote($item);
            }
            $insert[] = sprintf('(%s)', implode(',', $quotedValues));
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insert),
        )));

        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('id'),
                new BigqueryColumn('price', new Bigquery(
                    Bigquery::TYPE_NUMERIC,
                )),
            ]),
            [],
        );
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));

        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('id')
            ->setDestinationColumnName('id');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('price')
            ->setDestinationColumnName('price');
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
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE)
                ->setImportType($importType)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE), // <- just prove that this has no effect on import
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('should fail because of columns mismatch');
        } catch (ColumnsMismatchException $e) {
            $this->assertSame(
                'Source destination columns mismatch. "price STRING DEFAULT \'\' NOT NULL"->"price NUMERIC"',
                $e->getMessage(),
            );
        }
    }

    public function importTypeBoundsProvide(): Generator
    {
        yield 'full' => [
            'importType' => ImportOptions\ImportType::FULL,
            'longContent' => 'xxxyyyxxx',
        ];
        yield 'incremental' => [
            ImportOptions\ImportType::INCREMENTAL,
            'longContent' => 'xxxyyyxxx',
        ];
        yield 'full error import' => [
            'importType' => ImportOptions\ImportType::FULL,
            'longContent' => 'xxxyyyxxxyyyxxxyyyxxx',
        ];
        yield 'incremental error import' => [
            ImportOptions\ImportType::INCREMENTAL,
            'longContent' => 'xxxyyyxxxyyyxxxyyyxxx',
        ];
    }

    /**
     * @dataProvider importTypeBoundsProvide
     */
    public function testLoadDataToDifferentColumnLengthMismatchBounds(int $importType, string $longContent): void
    {
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
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('price'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));
        $insert = [];
        foreach ([['1', 'too expensive'], ['2', 'cheap'], ['3', $longContent]] as $i) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = BigqueryQuote::quote($item);
            }
            $insert[] = sprintf('(%s)', implode(',', $quotedValues));
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insert),
        )));

        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('id'),
                new BigqueryColumn('price', new Bigquery(
                    type: Bigquery::TYPE_STRING,
                    options: [
                        'length' => '20',
                    ],
                )),
            ]),
            [],
        );
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));

        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('id')
            ->setDestinationColumnName('id');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('price')
            ->setDestinationColumnName('price');
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
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE)
                ->setImportType($importType)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE), // <- just prove that this has no effect on import
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $response = null;
        try {
            /** @var TableImportResponse $response */
            $response = $handler(
                $this->projectCredentials,
                $cmd,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            if (strlen($longContent) === 21) {
                $this->fail('should fail because of column content won\'t fit');
            }
        } catch (MaximumLengthOverflowException $e) {
            $this->assertSame(
                sprintf('Field price: STRING(20) has maximum length 20 but got a value with length 21'),
                $e->getMessage(),
            );
        }
        if (strlen($longContent) !== 21) {
            $this->assertNotNull($response);
            $this->assertSame(3, $response->getImportedRowsCount());
        } else {
            $this->assertNull($response);
        }
    }

    public function testImportFromTypedTableToStringTable(): void
    {
        $sourceTableName = $this->getTestHash() . '_Test_table'; // workspace
        $destinationTableName = $this->getTestHash() . '_Test_table_final'; // storage
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // create source table with typed columns
        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                new BigqueryColumn('id', new Bigquery(
                    Bigquery::TYPE_INT,
                    [],
                )),
                new BigqueryColumn('time', new Bigquery(
                    Bigquery::TYPE_TIMESTAMP,
                    [],
                )),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));

        // insert test data
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES (1, TIMESTAMP "2023-01-01 12:00:00"), (2, TIMESTAMP "2023-01-01 12:00:00")',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
        )));

        // create destination table with string columns
        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                new BigqueryColumn('id', new Bigquery(
                    Bigquery::TYPE_STRING,
                    ['length' => '50'],
                )),
                new BigqueryColumn('time', new Bigquery(
                    Bigquery::TYPE_STRING,
                    ['length' => '50'],
                )),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            [],
        );
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));

        // prepare import command
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('id')
            ->setDestinationColumnName('id');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('time')
            ->setDestinationColumnName('time');
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
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setImportStrategy(ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp')
                ->setNumberOfIgnoredLines(0),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        // execute import
        // this is main purpose of the test, that import won't fail
        // internally ToStageImporter is used in this case and not CopyImportFromTableToTable
        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // verify results (not much important here)
        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT id, time FROM %s.%s ORDER BY id ASC',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));

        $rows = iterator_to_array($result);
        $this->assertCount(2, $rows);
        $this->assertIsArray($rows[0]);
        $this->assertIsArray($rows[1]);
        $this->assertArrayHasKey('id', $rows[0]);
        $this->assertArrayHasKey('time', $rows[0]);
        $this->assertArrayHasKey('id', $rows[1]);
        $this->assertArrayHasKey('time', $rows[1]);

        $this->assertSame('1', $rows[0]['id']);
        $this->assertSame('2023-01-01 12:00:00+00', $rows[0]['time']);
        $this->assertSame('2', $rows[1]['id']);
        $this->assertSame('2023-01-01 12:00:00+00', $rows[1]['time']);
    }
}
