<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace\Load;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
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

final class CastStringToTypedDestinationTest extends BaseImportTestCase
{
    public function testFullLoadWithoutTimestampAndInsertStrategy(): void
    {
        // this test covers the first step in handler
        $dataset = $this->bucketResponse->getCreateBucketObjectName();
        $src = $this->getTestHash() . '_cast_src';
        $dest = $this->getTestHash() . '_cast_dest';

        $bq = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $qb = new BigqueryTableQueryBuilder();

        // Clean up
        $this->dropTableIfExists($bq, $dataset, $src);
        $this->dropTableIfExists($bq, $dataset, $dest);

        // Source: all STRING columns
        $srcDef = new BigqueryTableDefinition(
            $dataset,
            $src,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'), // numbers as strings
                BigqueryColumn::createGenericColumn('col2'), // free text
                BigqueryColumn::createGenericColumn('col3'), // boolean-like strings
            ]),
            [],
        );
        $bq->runQuery($bq->query($qb->getCreateTableCommand(
            $srcDef->getSchemaName(),
            $srcDef->getTableName(),
            $srcDef->getColumnsDefinitions(),
            $srcDef->getPrimaryKeysNames(),
        )));

        // Insert sample rows
        $rows = [
            ['1', 'alpha', 'true'],
            ['2', 'beta', 'false'],
            ['3', 'gamma', 'true'],
        ];
        $values = [];
        foreach ($rows as $r) {
            $values[] = sprintf(
                '(%s, %s, %s)',
                BigqueryQuote::quote($r[0]),
                BigqueryQuote::quote($r[1]),
                BigqueryQuote::quote($r[2]),
            );
        }
        $bq->runQuery($bq->query(sprintf(
            'INSERT INTO %s.%s (col1, col2, col3) VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($dataset),
            BigqueryQuote::quoteSingleIdentifier($src),
            implode(',', $values),
        )));

        // Destination: typed columns (INT, STRING, BOOLEAN) + _timestamp
        $destDef = new BigqueryTableDefinition(
            $dataset,
            $dest,
            false,
            new ColumnCollection([
                new BigqueryColumn('col1', new Bigquery(Bigquery::TYPE_INT, [])),
                new BigqueryColumn('col2', new Bigquery(Bigquery::TYPE_STRING, [])),
                new BigqueryColumn('col3', new Bigquery(Bigquery::TYPE_BOOLEAN, [])),
            ]),
            [],
        );
        $bq->runQuery($bq->query($qb->getCreateTableCommand(
            $destDef->getSchemaName(),
            $destDef->getTableName(),
            $destDef->getColumnsDefinitions(),
            $destDef->getPrimaryKeysNames(),
        )));

        // Build command (FULL + timestamp to force SQL path; map 1:1)
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $dataset;
        $mappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        foreach (['col1', 'col2', 'col3'] as $c) {
            $mappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($c)
                ->setDestinationColumnName($c);
        }

        $cmd = (new LoadTableToWorkspaceCommand())
            ->setSource(
                (new LoadTableToWorkspaceCommand\SourceTableMapping())
                    ->setPath($path)
                    ->setTableName($src)
                    ->setColumnMappings($mappings),
            )
            ->setDestination(
                (new Table())
                    ->setPath($path)
                    ->setTableName($dest),
            )
            ->setImportOptions(
                (new ImportOptions())
                    ->setImportType(ImportOptions\ImportType::FULL)
                    ->setDedupType(ImportOptions\DedupType::INSERT_DUPLICATES)
                    ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                    ->setNumberOfIgnoredLines(0),
            );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $handler($this->projectCredentials, $cmd, [], new RuntimeOptions(['runId' => $this->testRunId]));

        $data = $this->fetchTable($bq, $dataset, $destDef->getTableName());
        usort($data, function ($a, $b) {
            return $a['col1'] <=> $b['col1'];
        });
        $this->assertEquals(
            [
                ['col1' => 1, 'col2' => 'alpha', 'col3' => true],
                ['col1' => 2, 'col2' => 'beta', 'col3' => false],
                ['col1' => 3, 'col2' => 'gamma', 'col3' => true],
            ],
            $data,
        );
    }
}
