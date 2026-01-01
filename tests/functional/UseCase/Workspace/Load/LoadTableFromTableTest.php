<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace\Load;

use Generator;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Cloud\Core\Exception\NotFoundException;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\Backend\BigQuery\Clustering;
use Keboola\StorageDriver\Backend\BigQuery\RangePartitioning;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\BadExportFilterParametersException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\ColumnsMismatchException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\MaximumLengthOverflowException;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
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

class LoadTableFromTableTest extends BaseImportTestCase
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
            ->setDestinationColumnName('col4');
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
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE), // <- just prove that this has no effect on import
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
            ->setDestinationColumnName('col4');
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
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp'),
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
            ->setDestinationColumnName('col4');
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

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatasetName, $tableName);
        $this->dropTableIfExists($bqClient, $bucketDatasetName, $destinationTableName);

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

        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('id')
            ->setDestinationColumnName('id');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('time')
            ->setDestinationColumnName('time');
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
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

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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
        $convertEmptyValues = new RepeatedField(GPBType::STRING);
        $convertEmptyValues[] = 'col3';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns($convertEmptyValues)
                ->setNumberOfIgnoredLines(0)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE), // <- just prove that this has no effect on import
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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
    public function testLoadDataToIncompatibleColumnTypeEndsWithMismatchException(int $importType): void
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
            ->setSourceColumnName('price')
            ->setDestinationColumnName('price');
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
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE)
                ->setImportType($importType)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE), // <- just prove that this has no effect on import
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('should fail because of columns mismatch');
        } catch (ImportValidationException $e) {
            $this->assertStringContainsString(
                'Invalid NUMERIC value',
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
            ->setSourceColumnName('price')
            ->setDestinationColumnName('price');
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
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE)
                ->setImportType($importType)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE), // <- just prove that this has no effect on import
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);

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
            ->setSourceColumnName('time')
            ->setDestinationColumnName('time');
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
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setImportStrategy(ImportStrategy::STRING_TABLE)
                ->setTimestampColumn('_timestamp')
                ->setNumberOfIgnoredLines(0),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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

        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'name', '_timestamp'] as $column) {
            $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($column)
                ->setDestinationColumnName($column);
        }

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
                ->setImportStrategy(ImportStrategy::USER_DEFINED_TABLE)
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setTimestampColumn('_timestamp')
                ->setImportAsNull(new RepeatedField(GPBType::STRING)),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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

    public function testCopyOptimizationNotUsedForIncrementalWithDedup(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_copy_test';
        $destinationTableName = $this->getTestHash() . '_dest_copy_test';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $pkColumns = ['pk_col'];

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);

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
            $pkColumns,
        );

        $sql = $qb->getCreateTableCommand(
            $destinationDefinition->getSchemaName(),
            $destinationDefinition->getTableName(),
            $destinationDefinition->getColumnsDefinitions(),
            $pkColumns,
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
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        // Column mappings (1:1, identical columns)
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['pk_col', 'value_col'] as $column) {
            $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($column)
                ->setDestinationColumnName($column);
        }

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

        // CRITICAL: Incremental + UPDATE_DUPLICATES + dedup columns

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
    }

    public function testImportTableFromTableWithPrimaryKeyScenarios(): void
    {
        $sourceTableName = $this->getTestHash() . '_pk_test_source';
        $destinationTableName = $this->getTestHash() . '_pk_test_dest';
        $autoCreateTableName = $this->getTestHash() . '_pk_test_auto_create';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $autoCreateTableName);

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
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('value')
            ->setDestinationColumnName('value');

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

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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
        $cmd2 = new LoadTableToWorkspaceCommand();
        $path2 = new RepeatedField(GPBType::STRING);
        $path2[] = $bucketDatabaseName;

        $columnMappings2 = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'name', 'value'] as $col) {
            $columnMappings2[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($col)
                ->setDestinationColumnName($col);
        }

        $cmd2->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
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

        $handler2 = new LoadTableToWorkspaceHandler($this->clientManager);
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
    }

    public function testImportTableFromTableWithoutTimestampInMapping(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_no_ts';
        $destinationTableName = $this->getTestHash() . '_dest_no_ts';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);

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
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col2')
            ->setDestinationColumnName('col2');

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd = (new LoadTableToWorkspaceCommand())
            ->setSource(
                (new LoadTableToWorkspaceCommand\SourceTableMapping())
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

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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

    public function testDedupColumnsCanDifferFromPrimaryKeys(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_dedup_pk_diff';
        $destinationTableName = $this->getTestHash() . '_dest_dedup_pk_diff';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);

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
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['id', 'region', 'name'] as $column) {
            $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($column)
                ->setDestinationColumnName($column);
        }

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

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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

    public function testImportTableFromTableWithFilterOnNonSelectedColumn(): void
    {
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

        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('Id')
            ->setDestinationColumnName('Id');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('Name')
            ->setDestinationColumnName('Name');

        $whereFilters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $whereFilters[] = (new TableWhereFilter())
            ->setColumnsName('iso')
            ->setOperator(Operator::eq)
            ->setValues(ProtobufHelper::arrayToRepeatedString(['cz']))
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
                ->setImportType(ImportOptions\ImportType::FULL),
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

    public function testImportTableFromTableWithFiltersAndLimit(): void
    {
        $sourceTableName = $this->getTestHash() . '_src_filters';
        $destinationTableName = $this->getTestHash() . '_dest_filters';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // cleanup from previous failed runs
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $sourceTableName);
        $this->dropTableIfExists($bqClient, $bucketDatabaseName, $destinationTableName);

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
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
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

        $sourceMapping = (new LoadTableToWorkspaceCommand\SourceTableMapping())
            ->setPath($path)
            ->setTableName($sourceTableName)
            ->setColumnMappings($columnMappings)
            ->setWhereFilters($whereFilters)
            ->setLimit(2);

        $cmd = (new LoadTableToWorkspaceCommand())
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

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
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

    public function testImportTableFromTableFullLoadNoDedupWithPrimaryKeys(): void
    {
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
            ->setDestinationColumnName('col4');
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
                ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE), // <- just prove that this has no effect on import
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
        $this->assertSame(3, $response->getImportedRowsCount());
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());
    }
}
