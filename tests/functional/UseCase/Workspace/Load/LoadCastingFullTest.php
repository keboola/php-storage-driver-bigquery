<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace\Load;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery as BigqueryType;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\DedupType;
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

final class LoadCastingFullTest extends BaseImportTestCase
{
    public static function fullProvider(): Generator
    {
        $srcTypedOptions = [true, false];
        $dataCastingOptions = [true, false];
        // here I test that src has timestamp (normal) or not (external datasets)
        // it isn't possible to create a table with TS in WS without CLONE operation
        $srcHasTS = [true, false];
        $renameColumns = [true, false];
        // this test does not check deduplication itself, just the impact of PK presence on import (different cases)
        $primaryKey = ['empty' => [], 'set' => ['id']];

        foreach ($srcTypedOptions as $srcTyped) {
            foreach ($dataCastingOptions as $dataCasting) {
                foreach ($srcHasTS as $srcWithTs) {
                    foreach ($primaryKey as $pkDescription => $pk) {
                        foreach ($renameColumns as $rename) {
                            if ($srcTyped && ($rename || $dataCasting)) {
                                // we cannot rename or cast columns when source is typed - not such case in connection
                                continue;
                            }
                            yield sprintf(
                                'src typed: %s | casting: %s | rename:%s | TS in SRC :%s | PK: %s',
                                $srcTyped ? 'Y' : 'N',
                                $dataCasting ? 'Y' : 'N',
                                $rename ? 'Y' : 'N',
                                $srcWithTs ? 'Y' : 'N',
                                $pkDescription,
                            ) => [
                                [
                                    'withSrcTimestamp' => $srcWithTs,
                                    'srcTyped' => $srcTyped,
                                    'dataCasting' => $dataCasting,
                                    'pk' => $pk,
                                    'allowRename' => $rename,
                                ],
                            ];
                        }
                    }
                }
            }
        }
    }

    /**
     * @param string[] $primaryKeys
     */
    private function createSourceDefinition(
        string $dataset,
        string $table,
        bool $srcTyped,
        bool $withTimestamp,
        array $primaryKeys,
    ): BigqueryTableDefinition {
        if (!$srcTyped) {
            $columns = [
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('text'),
                BigqueryColumn::createGenericColumn('flag'),
            ];
        } else {
            $columns = [
                new BigqueryColumn('id', new BigqueryType(BigqueryType::TYPE_INT, [])),
                new BigqueryColumn('text', new BigqueryType(BigqueryType::TYPE_STRING, [])),
                new BigqueryColumn('flag', new BigqueryType(BigqueryType::TYPE_BOOLEAN, [])),
            ];
        }

        if ($withTimestamp) {
            $columns[] = BigqueryColumn::createTimestampColumn('_timestamp');
        }

        return new BigqueryTableDefinition(
            $dataset,
            $table,
            false,
            new ColumnCollection($columns),
            $primaryKeys,
        );
    }

    /**
     * @dataProvider fullProvider
     * @param array{
     *       withSrcTimestamp: bool,
     *       srcTyped: bool,
     *       dataCasting: bool,
     *       pk: string[],
     *       allowRename: bool
     *   } $scenario
     */
    public function testFullLoadCasting(array $scenario): void
    {
        $dataset = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTable = $this->getTestHash() . '_full_src';
        $destTable = $this->getTestHash() . '_full_dest';

        $bq = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $qb = new BigqueryTableQueryBuilder();

        // Cleanup
        $this->dropTableIfExists($bq, $dataset, $sourceTable);
        $this->dropTableIfExists($bq, $dataset, $destTable);

        // Source
        $srcDef = $this->createSourceDefinition(
            $dataset,
            $sourceTable,
            $scenario['srcTyped'],
            $scenario['withSrcTimestamp'],
            $scenario['pk'],
        );
        $bq->runQuery($bq->query($qb->getCreateTableCommand(
            $srcDef->getSchemaName(),
            $srcDef->getTableName(),
            $srcDef->getColumnsDefinitions(),
            $srcDef->getPrimaryKeysNames(),
        )));
        $this->insertSourceRows($bq, $dataset, $sourceTable, $scenario['srcTyped']);

        // Destination
        $destDef = $this->createDestinationDefinition(
            $dataset,
            $destTable,
            ($scenario['dataCasting'] || $scenario['srcTyped']),
            $scenario['allowRename'],
        );
        $bq->runQuery($bq->query($qb->getCreateTableCommand(
            $destDef->getSchemaName(),
            $destDef->getTableName(),
            $destDef->getColumnsDefinitions(),
            $destDef->getPrimaryKeysNames(),
        )));

        // Build command
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $dataset;
        $mappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $destColumns = $destDef->getColumnsNames();
        foreach ($srcDef->getColumnsNames() as $key => $colName) {
            if ($colName === '_timestamp') {
                continue;
            }
            $mappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($colName)
                ->setDestinationColumnName($destColumns[$key]);
        }

        $cmd = (new LoadTableToWorkspaceCommand())
            ->setSource(
                (new LoadTableToWorkspaceCommand\SourceTableMapping())
                    ->setPath($path)
                    ->setTableName($sourceTable)
                    ->setColumnMappings($mappings),
            )
            ->setDestination((new Table())->setPath($path)->setTableName($destTable));

        $importStrategy = $scenario['srcTyped'] ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE;
        $dedupType = $scenario['pk'] === [] ? DedupType::INSERT_DUPLICATES : DedupType::UPDATE_DUPLICATES;
        $options = (new ImportOptions())
            ->setImportType(ImportOptions\ImportType::FULL)
            ->setDedupType($dedupType)
            ->setImportStrategy($importStrategy)
            ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
            ->setNumberOfIgnoredLines(0);
        // we do not set timestamp (setTimestampColumn('_timestamp')) column because COPY won't allow creating TS
        if ($scenario['pk'] !== []) {
            $dedup = new RepeatedField(GPBType::STRING);
            foreach ($scenario['pk'] as $pk) {
                $dedup[] = $pk;
            }
            $options->setDedupColumnsNames($dedup);
        }
        $cmd->setImportOptions($options);

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        /** @var TableImportResponse $response */
        $response = $handler($this->projectCredentials, $cmd, [], new RuntimeOptions(['runId' => $this->testRunId]));

        $ref = new BigqueryTableReflection($bq, $dataset, $destTable);
        $this->assertSame(3, $ref->getRowsCount());

        $flagName = $scenario['allowRename'] ? 'flag_renamed' : 'flag';
        // types have been casted or they were defined on src
        $data = $this->fetchTable($bq, $dataset, $destTable, ['id', 'text', $flagName]);
        usort($data, function ($a, $b) {
            return $a['id'] <=> $b['id'];
        });
        if ($scenario['dataCasting'] || $scenario['srcTyped']) {
            $this->assertEquals(
                [
                    ['id' => 1, 'text' => 'alpha', $flagName => true],
                    ['id' => 2, 'text' => 'beta', $flagName => false],
                    ['id' => 3, 'text' => 'gamma', $flagName => true],
                ],
                $data,
            );
        } else {
            // types to be string in destination
            $this->assertEquals(
                [
                    ['id' => '1', 'text' => 'alpha', $flagName => 'true'],
                    ['id' => '2', 'text' => 'beta', $flagName => 'false'],
                    ['id' => '3', 'text' => 'gamma', $flagName => 'true'],
                ],
                $data,
            );
        }
    }

    private function createDestinationDefinition(
        string $dataset,
        string $table,
        bool $destTyped,
        bool $allowRename,
    ): BigqueryTableDefinition {
        $flagColumnName = $allowRename ? 'flag_renamed' : 'flag';
        if (!$destTyped) {
            $columns = [
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('text'),
                BigqueryColumn::createGenericColumn($flagColumnName),
            ];
        } else {
            $columns = [
                new BigqueryColumn('id', new BigqueryType(BigqueryType::TYPE_INT, [])),
                new BigqueryColumn('text', new BigqueryType(BigqueryType::TYPE_STRING, [])),
                new BigqueryColumn($flagColumnName, new BigqueryType(BigqueryType::TYPE_BOOLEAN, [])),
            ];
        }

        return new BigqueryTableDefinition(
            $dataset,
            $table,
            false,
            new ColumnCollection($columns),
            [],
        );
    }

    private function insertSourceRows(
        BigQueryClient $bq,
        string $dataset,
        string $table,
        bool $srcTyped,
    ): void {
        if (!$srcTyped) {
            $values = [
                sprintf(
                    '(%s,%s,%s)',
                    BigqueryQuote::quote('1'),
                    BigqueryQuote::quote('alpha'),
                    BigqueryQuote::quote('true'),
                ),
                sprintf(
                    '(%s,%s,%s)',
                    BigqueryQuote::quote('2'),
                    BigqueryQuote::quote('beta'),
                    BigqueryQuote::quote('false'),
                ),
                sprintf(
                    '(%s,%s,%s)',
                    BigqueryQuote::quote('3'),
                    BigqueryQuote::quote('gamma'),
                    BigqueryQuote::quote('true'),
                ),
            ];
        } else {
            $values = [
                sprintf('(1,%s,TRUE)', BigqueryQuote::quote('alpha')),
                sprintf('(2,%s,FALSE)', BigqueryQuote::quote('beta')),
                sprintf('(3,%s,TRUE)', BigqueryQuote::quote('gamma')),
            ];
        }

        $bq->runQuery($bq->query(sprintf(
            'INSERT INTO %s.%s (id, text, flag) VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($dataset),
            BigqueryQuote::quoteSingleIdentifier($table),
            implode(',', $values),
        )));
    }
}
