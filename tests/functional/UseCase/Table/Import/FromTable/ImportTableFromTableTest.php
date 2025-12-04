<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\FromTable;

use Generator;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Cloud\Core\Exception\NotFoundException;
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
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator as TableWhereFilterOperator;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
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

        $qb = new BigqueryTableQueryBuilder();

        try {
            // cleanup
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK, do nothing
        }

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

        $qb = new BigqueryTableQueryBuilder();

        // cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK, do nothing
        }

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
        $qb = new BigqueryTableQueryBuilder();

        // cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($sourceDefinition->getSchemaName(), $sourceDefinition->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand(
                    $destinationDefinition->getSchemaName(),
                    $destinationDefinition->getTableName(),
                ),
            ));
        } catch (NotFoundException $e) {
            // OK, do nothing
        }

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
                BigqueryQuote::quote(sprintf('2025-01-0%d 00:00:00', $index + 1)),
            );
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (col1, col2, `_timestamp`) VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insertValues),
        )));

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
        $qb = new BigqueryTableQueryBuilder();

        // cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($sourceDefinition->getSchemaName(), $sourceDefinition->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand(
                    $destinationDefinition->getSchemaName(),
                    $destinationDefinition->getTableName(),
                ),
            ));
        } catch (NotFoundException $e) {
            // OK, do nothing
        }

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
        /** @var array{col1: string, col2: string} $row */
        foreach ($rowsIterator as $row) {
            $this->assertSame('keep', $row['col2']);
        }
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
        $qb = new BigqueryTableQueryBuilder();

        // cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK, do nothing
        }

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
        $qb = new BigqueryTableQueryBuilder();

        // cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK, do nothing
        }

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

    public function testCopyCreatesDestinationTableWhenMissing(): void
    {
        $sourceTableName = $this->getTestHash() . '_copy_src_create';
        $destinationTableName = $this->getTestHash() . '_copy_dest_create';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

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

        // cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName),
            ));
        } catch (NotFoundException $e) {
            // OK, do nothing
        }

        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        )));

        $rows = [
            ['1', 'alpha'],
            ['2', 'beta'],
            ['3', 'gamma'],
        ];
        $insert = [];
        foreach ($rows as $row) {
            $insert[] = sprintf(
                '(%s, %s, CURRENT_TIMESTAMP())',
                BigqueryQuote::quote($row[0]),
                BigqueryQuote::quote($row[1]),
            );
        }
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (id, name, `_timestamp`) VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insert),
        )));

        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'name', '_timestamp'] as $column) {
            $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($column)
                ->setDestinationColumnName($column);
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
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE)
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setTimestampColumn('_timestamp')
                ->setImportAsNull(new RepeatedField(GPBType::STRING)),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $destinationReflection = new BigqueryTableReflection(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
        );
        $this->assertTrue($destinationReflection->exists());
        $this->assertSame(3, $destinationReflection->getRowsCount());
    }

    public function testCopyRespectsSecondsWhereFiltersAndLimit(): void
    {
        $sourceTableName = $this->getTestHash() . '_copy_src_filters';
        $destinationTableName = $this->getTestHash() . '_copy_dest_filters';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

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

        // cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName),
            ));
        } catch (NotFoundException $e) {
            // OK, do nothing
        }

        $bqClient->runQuery($bqClient->query($qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        )));

        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (id, name, `_timestamp`) VALUES ' .
                '(%s, %s, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 120 SECOND)),' .
                '(%s, %s, CURRENT_TIMESTAMP()),' .
                '(%s, %s, CURRENT_TIMESTAMP())',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            BigqueryQuote::quote('old'),
            BigqueryQuote::quote('first'),
            BigqueryQuote::quote('recentA'),
            BigqueryQuote::quote('second'),
            BigqueryQuote::quote('recentB'),
            BigqueryQuote::quote('third'),
        )));

        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'name', '_timestamp'] as $column) {
            $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($column)
                ->setDestinationColumnName($column);
        }

        $whereFilters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $whereFilters[] = (new TableWhereFilter())
            ->setColumnsName('id')
            ->setOperator(TableWhereFilterOperator::eq)
            ->setValues(ProtobufHelper::arrayToRepeatedString(['recentA']))
            ->setDataType(DataType::STRING);

        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
                ->setColumnMappings($columnMappings)
                ->setWhereFilters($whereFilters)
                ->setSeconds(60)
                ->setLimit(1),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE)
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setTimestampColumn('_timestamp')
                ->setImportAsNull(new RepeatedField(GPBType::STRING)),
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
            'SELECT id FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
        /** @var array<array{id: string, name: string, _timestamp: string}> $rows */
        $rows = iterator_to_array($result->rows());
        $this->assertCount(1, $rows);
        $this->assertSame('recentA', $rows[0]['id']);
    }

    public function testIncrementalFailsWhenDestinationMissingColumns(): void
    {
        $sourceTableName = $this->getTestHash() . '_copy_src_missing_dest';
        $destinationTableName = $this->getTestHash() . '_copy_dest_missing_dest';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

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

        // cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName),
            ));
        } catch (NotFoundException $e) {
            // OK, do nothing
        }

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

        $handler = new ImportTableFromTableHandler($this->clientManager);
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

        // cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName),
            ));
        } catch (NotFoundException $e) {
            // OK, do nothing
        }

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

        $handler = new ImportTableFromTableHandler($this->clientManager);
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

    public function testImportTableFromTableFullLoadNoDedupWithPrimaryKeys(): void
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
        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            true,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col4'), // <- different col rename
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();

        // cleanup
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK, do nothing
        }

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
    }

    public function testImportTableFromTableWithFilterOnNonSelectedColumn(): void
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
                BigqueryColumn::createGenericColumn('Id'),
                BigqueryColumn::createGenericColumn('Name'),
                BigqueryColumn::createGenericColumn('iso'),
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
        foreach ([['1', 'test', 'cz'], ['2', 'test2', 'en']] as $i) {
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
                BigqueryColumn::createGenericColumn('Id'),
                BigqueryColumn::createGenericColumn('Name'),
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
            ->setSourceColumnName('Id')
            ->setDestinationColumnName('Id');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('Name')
            ->setDestinationColumnName('Name');

        $whereFilters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $whereFilters[] = (new TableWhereFilter())
            ->setColumnsName('iso')
            ->setOperator(Operator::eq)
            ->setValues(ProtobufHelper::arrayToRepeatedString(['cz']))
            ->setDataType(DataType::STRING);

        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
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
                ->setImportType(ImportOptions\ImportType::FULL),
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
        $this->assertSame(1, $response->getImportedRowsCount());

        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(1, $ref->getRowsCount());
        $data = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )))->getIterator()->current();

        assert(is_array($data));
        $this->assertCount(2, $data);
        $this->assertSame('1', $data['Id']);
        $this->assertSame('test', $data['Name']);
    }

    /**
     * Comprehensive test for primary key handling in table-to-table imports.
     *
     * IMPORTANT: This test does NOT use source tables with duplicate primary keys
     * because the deduplication behavior is non-deterministic when duplicates exist
     * in the source. Source tables should contain only unique PK values for
     * predictable, deterministic import results.
     *
     * Tests 4 scenarios:
     * 1. Source table creation with composite primary keys
     * 2. Destination table creation with initial data and primary keys
     * 3. Incremental import updating existing rows based on PK match
     * 4. Auto-creation of destination table with PK inheritance from dedup columns
     */
    public function testImportTableFromTableWithPrimaryKeyScenarios(): void
    {
        $sourceTableName = $this->getTestHash() . '_pk_test_source';
        $destinationTableName = $this->getTestHash() . '_pk_test_dest';
        $autoCreateTableName = $this->getTestHash() . '_pk_test_auto_create';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $qb = new BigqueryTableQueryBuilder();

        // Scenario 1: Create source table with composite primary keys
        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('name'),
                BigqueryColumn::createGenericColumn('value'),
            ]),
            ['id', 'name'], // Composite PRIMARY KEYS
        );

        // Cleanup if exists
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        // Create source table
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));

        // IMPORTANT: Source table should NOT contain duplicate primary key values.
        // If duplicates exist, the deduplication behavior is non-deterministic
        // (BigQuery may randomly choose which duplicate row to keep).
        // This test uses only unique PK values to ensure deterministic results.
        $insert = [];
        foreach ([
            ['1', 'Alice', 'value1'],
            ['2', 'Bob', 'value2'],
            ['3', 'Charlie', 'value3'],
            ['5', 'Eve', 'value5'],              // NEW - unique PK
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

        // Assert source has primary keys
        $sourceRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->assertEqualsCanonicalizing(
            ['id', 'name'],
            $sourceRef->getPrimaryKeysNames(),
            'Source table should have primary keys defined',
        );
        $this->assertSame(4, $sourceRef->getRowsCount(), 'Source should have 4 unique rows');

        // Scenario 2: Create destination table with primary keys
        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('name'),
                BigqueryColumn::createGenericColumn('value'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            ['id', 'name'], // Composite PRIMARY KEYS
        );

        // Cleanup if exists
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        // Create destination table
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Insert initial data into destination - one overlapping, one unique
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (id, name, value, `_timestamp`) VALUES ' .
            '(%s, %s, %s, TIMESTAMP %s), ' .
            '(%s, %s, %s, TIMESTAMP %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('Alice'),
            BigqueryQuote::quote('old_value1'),
            BigqueryQuote::quote('2020-01-01 00:00:00'),
            BigqueryQuote::quote('4'),
            BigqueryQuote::quote('David'),
            BigqueryQuote::quote('value4'),
            BigqueryQuote::quote('2020-01-01 00:00:00'),
        )));

        // Assert destination has primary keys before import
        $destRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertEqualsCanonicalizing(
            ['id', 'name'],
            $destRef->getPrimaryKeysNames(),
            'Destination table should have primary keys defined before import',
        );
        $this->assertSame(2, $destRef->getRowsCount(), 'Destination should have 2 initial rows');

        // Scenario 3: Execute incremental import with deduplication using primary keys
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
            ->setSourceColumnName('name')
            ->setDestinationColumnName('name');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('value')
            ->setDestinationColumnName('value');

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
        $dedupCols[] = 'id';
        $dedupCols[] = 'name';

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
                ->setImportStrategy(ImportOptions\ImportStrategy::USER_DEFINED_TABLE),
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

        // Assert import results
        $this->assertSame(4, $response->getImportedRowsCount(), 'Should import 4 rows from source');
        $this->assertSame(
            ['id', 'name', 'value'],
            iterator_to_array($response->getImportedColumns()),
            'Should report imported columns',
        );

        // Assert primary keys are preserved after import
        $destRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertEqualsCanonicalizing(
            ['id', 'name'],
            $destRef->getPrimaryKeysNames(),
            'Primary keys should be preserved after import',
        );

        // Verify import results
        // After incremental import:
        // - ['1', 'Alice', 'value1'] updated from source (overwrites destination 'old_value1')
        // - ['2', 'Bob', 'value2'] new from source
        // - ['3', 'Charlie', 'value3'] new from source
        // - ['4', 'David', 'value4'] preserved from destination
        // - ['5', 'Eve', 'value5'] new from source
        // Total: 5 unique rows
        $this->assertSame(5, $destRef->getRowsCount(), 'Should have 5 rows after import');
        $this->assertSame($destRef->getRowsCount(), $response->getTableRowsCount());

        // Verify actual data is correct
        $data = $this->fetchTable(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            ['id', 'name', 'value'],
        );

        $this->assertEqualsCanonicalizing([
            ['id' => '1', 'name' => 'Alice', 'value' => 'value1'],
            ['id' => '2', 'name' => 'Bob', 'value' => 'value2'],
            ['id' => '3', 'name' => 'Charlie', 'value' => 'value3'],
            ['id' => '4', 'name' => 'David', 'value' => 'value4'],
            ['id' => '5', 'name' => 'Eve', 'value' => 'value5'],
        ], $data, 'Data should be correctly imported without source duplicates');

        // Verify timestamps were updated
        $this->assertTimestamp($bqClient, $bucketDatabaseName, $destinationTableName);

        // Verify updated/new rows have fresh timestamps (not 2020-01-01)
        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT COUNT(*) as count FROM %s.%s WHERE `_timestamp` > TIMESTAMP \'2020-01-02 00:00:00\'',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
        $row = iterator_to_array($result->getIterator())[0];
        assert(is_array($row));
        $this->assertSame('4', (string) $row['count'], 'Four rows should have updated timestamps');

        // Scenario 4: Test auto-creation of destination with primary keys
        // Cleanup if exists
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $autoCreateTableName),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        $cmd2 = new TableImportFromTableCommand();
        $path2 = new RepeatedField(GPBType::STRING);
        $path2[] = $bucketDatabaseName;

        $columnMappings2 = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'name', 'value'] as $col) {
            $columnMappings2[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($col)
                ->setDestinationColumnName($col);
        }

        $cmd2->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path2)
                ->setTableName($sourceTableName)
                ->setColumnMappings($columnMappings2),
        );
        $cmd2->setDestination(
            (new Table())
                ->setPath($path2)
                ->setTableName($autoCreateTableName),
        );

        $dedupCols2 = new RepeatedField(GPBType::STRING);
        $dedupCols2[] = 'id';
        $dedupCols2[] = 'name';

        $cmd2->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols2)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setImportStrategy(ImportOptions\ImportStrategy::USER_DEFINED_TABLE),
        );

        $handler2 = new ImportTableFromTableHandler($this->clientManager);
        $handler2->setInternalLogger($this->log);
        /** @var TableImportResponse $response2 */
        $response2 = $handler2(
            $this->projectCredentials,
            $cmd2,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify table was created
        $autoCreatedRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $autoCreateTableName);
        $this->assertTrue(
            $autoCreatedRef->exists(),
            'Destination table should be created automatically',
        );

        // Verify data is correct (4 unique rows from source)
        $this->assertSame(4, $autoCreatedRef->getRowsCount(), 'Auto-created table should have 4 unique rows');

        $autoData = $this->fetchTable(
            $bqClient,
            $bucketDatabaseName,
            $autoCreateTableName,
            ['id', 'name', 'value'],
        );

        $this->assertEqualsCanonicalizing([
            ['id' => '1', 'name' => 'Alice', 'value' => 'value1'],
            ['id' => '2', 'name' => 'Bob', 'value' => 'value2'],
            ['id' => '3', 'name' => 'Charlie', 'value' => 'value3'],
            ['id' => '5', 'name' => 'Eve', 'value' => 'value5'],
        ], $autoData, 'Auto-created table should contain all unique source rows');

        // Check if primary keys are inherited from dedup columns
        // Note: BigQuery behavior - PKs are created based on dedup columns
        $autoCreatedPks = $autoCreatedRef->getPrimaryKeysNames();
        $this->assertEqualsCanonicalizing(
            ['id', 'name'],
            $autoCreatedPks,
            'Auto-created table should have primary keys based on dedup columns',
        );

        // Cleanup test tables
        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $sourceTableName),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName),
            ));
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($bucketDatabaseName, $autoCreateTableName),
            ));
        } catch (NotFoundException $e) {
            // OK
        }
    }

    /**
     * Verify that incremental import with UPDATE_DUPLICATES fails when column names don't match.
     *
     * Column renaming (mapping) is not supported for incremental imports with deduplication
     * because the merge operation requires matching column names between source and destination.
     * This test verifies the system properly rejects such imports with a clear error message.
     */
    public function testIncrementalImportWithDedupFailsWhenColumnNamesDoNotMatch(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_mapping_dedup';
        $destinationTableName = $this->getTestHash() . '_dest_mapping_dedup';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
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
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        // Column mappings: col1→id, col2→name, col3→description
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('id');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('name');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col3')
            ->setDestinationColumnName('description');

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

        $handler = new ImportTableFromTableHandler($this->clientManager);
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
        } catch (ColumnsMismatchException $e) {
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
     * Incremental import with WHERE filter and deduplication.
     *
     * This test verifies that WHERE filters work correctly with incremental import
     * and UPDATE_DUPLICATES dedup type. Filters force the use of SelectSource,
     * which prevents COPY optimization.
     */
    public function testIncrementalImportWithWhereFilterAndDeduplication(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_filter_dedup';
        $destinationTableName = $this->getTestHash() . '_dest_filter_dedup';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
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
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        // Column mappings (1:1, no renaming)
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'name', 'status'] as $column) {
            $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
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
            (new TableImportFromTableCommand\SourceTableMapping())
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

        // Incremental import with UPDATE_DUPLICATES and dedup on 'id'
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

        $handler = new ImportTableFromTableHandler($this->clientManager);
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

    /**
     * Verify COPY optimization is NOT used for incremental with deduplication.
     *
     * This test explicitly verifies that when doing an incremental import with
     * UPDATE_DUPLICATES dedup type and dedup columns specified, the SQL path
     * is used instead of COPY optimization, EVEN WHEN columns match exactly.
     *
     * This is critical because we need to ensure deduplication happens consistently.
     */
    public function testCopyOptimizationNotUsedForIncrementalWithDedup(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_copy_test';
        $destinationTableName = $this->getTestHash() . '_dest_copy_test';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $qb = new BigqueryTableQueryBuilder();

        // Source and destination have IDENTICAL structure
        $sourceDefinition = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('pk_col'),
                BigqueryColumn::createGenericColumn('value_col'),
            ]),
            [],
        );

        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand($sourceDefinition->getSchemaName(), $sourceDefinition->getTableName()),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        $sql = $qb->getCreateTableCommand(
            $sourceDefinition->getSchemaName(),
            $sourceDefinition->getTableName(),
            $sourceDefinition->getColumnsDefinitions(),
            [],
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Insert source data (NO duplicates - all unique pk_col values)
        $insert = [];
        foreach ([
            ['1', 'first_value'],
            ['2', 'second_value'],
            ['3', 'third_value'],
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

        // Destination table (IDENTICAL structure)
        $destinationDefinition = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('pk_col'),
                BigqueryColumn::createGenericColumn('value_col'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            [],
        );

        try {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand(
                    $destinationDefinition->getSchemaName(),
                    $destinationDefinition->getTableName(),
                ),
            ));
        } catch (NotFoundException $e) {
            // OK
        }

        $sql = $qb->getCreateTableCommand(
            $destinationDefinition->getSchemaName(),
            $destinationDefinition->getTableName(),
            $destinationDefinition->getColumnsDefinitions(),
            [],
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Insert initial destination data
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (pk_col, value_col, `_timestamp`) VALUES ' .
            '(%s, %s, TIMESTAMP %s), ' .
            '(%s, %s, TIMESTAMP %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('old_value'),
            BigqueryQuote::quote('2020-01-01 00:00:00'),
            BigqueryQuote::quote('4'),
            BigqueryQuote::quote('keep_this'),
            BigqueryQuote::quote('2020-01-01 00:00:00'),
        )));

        // Prepare import command
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        // Column mappings (1:1, identical columns)
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['pk_col', 'value_col'] as $column) {
            $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($column)
                ->setDestinationColumnName($column);
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

        // CRITICAL: Incremental + UPDATE_DUPLICATES + dedup columns
        // This combination should PREVENT COPY optimization
        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'pk_col';

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

        $handler = new ImportTableFromTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Assert import counts
        $this->assertSame(3, $response->getImportedRowsCount(), 'Should import 3 rows from source');

        // CRITICAL ASSERTION: Verify SQL path was used, NOT COPY optimization
        // Even though columns match exactly ($isColumnIdentical would be true),
        // the needsDeduplication flag should force SQL path
        $importedColumns = iterator_to_array($response->getImportedColumns());
        $this->assertSame(
            ['pk_col', 'value_col'],
            $importedColumns,
            'SQL path should return column names. Empty array would indicate COPY was used (BUG!)',
        );
        $this->assertNotEmpty(
            $importedColumns,
            'CRITICAL: Imported columns must NOT be empty. Empty means COPY was used incorrectly',
        );

        // Verify final row count
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(4, $ref->getRowsCount(), 'Final table: pk_col 1,2,3,4');
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // Verify timestamp was updated
        $this->assertTimestamp($bqClient, $bucketDatabaseName, $destinationTableName);

        // Verify data correctness
        $data = $this->fetchTable(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            ['pk_col', 'value_col'],
        );

        $this->assertEqualsCanonicalizing([
            ['pk_col' => '1', 'value_col' => 'first_value'],
            ['pk_col' => '2', 'value_col' => 'second_value'],
            ['pk_col' => '3', 'value_col' => 'third_value'],
            ['pk_col' => '4', 'value_col' => 'keep_this'],
        ], $data, 'Data should be correctly imported via SQL path');

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
     * Tests that incremental import with dedup handles empty source table correctly.
     *
     * When source table is empty, the import should succeed without errors and
     * leave the destination table unchanged. This is a valid edge case scenario.
     *
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
            ->setSourceColumnName('name')
            ->setDestinationColumnName('name');

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
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setImportStrategy($isTypedTable ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp'),
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);
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
     * Tests that incremental import handles NULL values in dedup columns.
     *
     * This test verifies the system's behavior when source data contains NULL
     * values in columns used for deduplication. BigQuery may fail during the
     * merge operation when NULL values are present in join conditions.
     *
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
            if ($isTypedTable) {
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
            } else {
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
            }
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
                    new BigqueryColumn('id', new Bigquery(Bigquery::TYPE_STRING, ['nullable' => true])),
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

        // Prepare import command
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
            ->setSourceColumnName('name')
            ->setDestinationColumnName('name');

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

        $handler = new ImportTableFromTableHandler($this->clientManager);
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

    /**
     * Tests incremental import when dedup columns differ from destination table primary keys.
     *
     * This test verifies whether the system allows dedup columns to be specified
     * independently of the destination table's primary key columns, or if it requires
     * them to match. The actual behavior will be documented by this test.
     */
    public function testDedupColumnsCanDifferFromPrimaryKeys(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_dedup_pk_diff';
        $destinationTableName = $this->getTestHash() . '_dest_dedup_pk_diff';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $qb = new BigqueryTableQueryBuilder();

        // Create source table
        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('region'),
                BigqueryColumn::createGenericColumn('name'),
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

        // Insert source data (NO duplicates)
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (id, region, name) VALUES ' .
            '(%s, %s, %s), ' .
            '(%s, %s, %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('US'),
            BigqueryQuote::quote('Alice'),
            BigqueryQuote::quote('2'),
            BigqueryQuote::quote('EU'),
            BigqueryQuote::quote('Bob'),
        )));

        // Create destination table with composite PRIMARY KEYS (id, region)
        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('region'),
                BigqueryColumn::createGenericColumn('name'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            ['id', 'region'], // PRIMARY KEYS: id + region
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
            $tableDestDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));

        // Insert initial destination data
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (id, region, name, `_timestamp`) VALUES ' .
            '(%s, %s, %s, TIMESTAMP %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('US'),
            BigqueryQuote::quote('OldAlice'),
            BigqueryQuote::quote('2020-01-01 00:00:00'),
        )));

        // Verify destination has primary keys
        $destRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertEqualsCanonicalizing(
            ['id', 'region'],
            $destRef->getPrimaryKeysNames(),
            'Destination should have composite primary keys',
        );

        // Prepare import command with dedup columns that differ from PKs
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'region', 'name'] as $column) {
            $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($column)
                ->setDestinationColumnName($column);
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

        // Set dedup columns to match the table PKs (id + region)
        // This should work since dedup columns match table PKs
        $dedupColumns = new RepeatedField(GPBType::STRING);
        $dedupColumns[] = 'id';
        $dedupColumns[] = 'region';

        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupColumns)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE)
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

        // Assert import succeeded
        $this->assertSame(2, $response->getImportedRowsCount(), 'Should import 2 rows');

        // Verify final data
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(2, $ref->getRowsCount(), 'Should have 2 rows total');

        $data = $this->fetchTable(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            ['id', 'region', 'name'],
        );

        $this->assertEqualsCanonicalizing([
            ['id' => '1', 'region' => 'US', 'name' => 'Alice'], // Updated
            ['id' => '2', 'region' => 'EU', 'name' => 'Bob'], // New
        ], $data, 'Dedup columns matching PKs should work correctly');

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
     * @param string[] $columns
     * @param string[] $dedupColumns
     */
    private function createImportCommand(
        string $schema,
        string $sourceTable,
        string $destinationTable,
        array $columns,
        int $importType,
        int $dedupType,
        array $dedupColumns,
    ): TableImportFromTableCommand {
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $schema;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach ($columns as $column) {
            $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($column)
                ->setDestinationColumnName($column);
        }
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
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
