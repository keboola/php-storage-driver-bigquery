<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\FromTable;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

class IncrementalImportTableFromTableTest extends BaseImportTestCase
{
    /**
     * Incremental load to storage from workspace
     * This is output mapping, timestamp is updated
     */
    public function testImportTableFromTableIncrementalLoad(): void
    {
        $sourceTableName = md5($this->getName()) . '_Test_table';
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);

        // create tables
        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                new BigqueryColumn(
                    'col1',
                    new Bigquery(Bigquery::TYPE_STRING, [
                        'length' => '32000',
                        'nullable' => false,
                    ])
                ),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createGenericColumn('col3'),
            ]),
            ['col1']
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            [], //<-- dont create primary keys allow duplicates
        );
        $bqClient->runQuery($bqClient->query($sql));
        foreach ([['1', '1', '3'], ['2', '2', '2'], ['2', '2', '2'], ['3', '2', '3'], ['4', '4', '4']] as $i) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = BigqueryQuote::quote($item);
            }
            $bqClient->runQuery($bqClient->query(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($sourceTableName),
                implode(',', $quotedValues)
            )));
        }

        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                new BigqueryColumn(
                    'col1',
                    new Bigquery(Bigquery::TYPE_STRING, [
                        'length' => '32000',
                        'nullable' => false,
                    ])
                ),
                BigqueryColumn::createGenericColumn('col4'), // <- different col rename
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            ['col1']
        );
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            [],
        );
        $bqClient->runQuery($bqClient->query($sql));
        foreach ([
                     ['1', '1', '2022-11-23 14:46:00'],
                     ['2', '2', '2022-11-23 14:47:00'],
                     ['3', '3', '2022-11-23 14:48:00'],
                 ] as $i
        ) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = BigqueryQuote::quote($item);
            }
            $bqClient->runQuery($bqClient->query(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($destinationTableName),
                implode(',', $quotedValues)
            )));
        }

        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class
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
                ->setColumnMappings($columnMappings)
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $dedupCols[] = 'col1';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setDedupColumnsNames($dedupCols)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromTableHandler($this->clientManager);

        $handler(
            $this->projectCredentials,
            $cmd,
            []
        );
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
//             1 row unique from source, 3 rows deduped from source and destination
        $this->assertSame(4, $ref->getRowsCount());
        $this->assertTimestamp($bqClient, $bucketDatabaseName, $destinationTableName);
        // @todo test updated values

        // cleanup
        $qb->getDropTableCommand($tableSourceDef->getSchemaName(), $tableSourceDef->getTableName());
        $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
    }
}
