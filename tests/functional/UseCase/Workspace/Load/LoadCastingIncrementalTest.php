<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace\Load;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery as BigqueryType;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\ColumnsMismatchException as DriverColumnsMismatchException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\DedupType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

final class LoadCastingIncrementalTest extends BaseImportTestCase
{
    public static function incrementalProvider(): Generator
    {
        $srcTypedOptions = [true, false];
        $dataCastingOptions = [true, false];
        // here I test that src has timestamp (normal) or not (external datasets)
        // it isn't possible to create a table with TS in WS without CLONE operation
        $srcHasTS = [true, false];
        $renameColumns = [true, false];
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
     * @dataProvider incrementalProvider
     * @param array{
     *       withSrcTimestamp: bool,
     *       srcTyped: bool,
     *       dataCasting: bool,
     *       pk: string[],
     *       allowRename: bool
     *   } $scenario
     */
    public function testIncrementalLoadCasting(array $scenario): void
    {
        $dataset = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTable = $this->getTestHash() . '_incr_src';
        $destTable = $this->getTestHash() . '_incr_dest';
        $pkSet = $scenario['pk'] !== [];
        $bq = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $qb = new BigqueryTableQueryBuilder();

        // Cleanup
        $this->dropTableIfExists($bq, $dataset, $sourceTable);
        $this->dropTableIfExists($bq, $dataset, $destTable);

        // Source (no PKs on BQ side, dedup works logically)
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
            [],
        )));
        $this->insertSourceRows($bq, $dataset, $sourceTable, $scenario['srcTyped']);

        // Destination (add _timestamp when requested)
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
            [],
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
        $dedupType = $scenario['pk'] === [] ? DedupType::INSERT_DUPLICATES : DedupType::UPDATE_DUPLICATES;
        $importStrategy = $scenario['srcTyped'] ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE;

        $options = (new ImportOptions())
            ->setImportType(ImportOptions\ImportType::INCREMENTAL)
            ->setDedupType($dedupType)
            ->setImportStrategy($importStrategy)
            ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
            ->setNumberOfIgnoredLines(0);
//        if ($scenario['withTimestamp']) {
//            $options->setTimestampColumn('_timestamp');
//        }

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

        $response = $handler($this->projectCredentials, $cmd, [], new RuntimeOptions(['runId' => $this->testRunId]));

        // TODO?
//        if ($scenario['withSrcTimestamp']) {
//            $this->assertTimestamp($bq, $dataset, $destTable);
//        }

        $flagName = $scenario['allowRename'] ? 'flag_renamed' : 'flag';
        // types have been casted or they were defined on src
        $data = $this->fetchTable($bq, $dataset, $destTable, ['id', 'text', $flagName]);
        usort($data, function ($a, $b) {
            return $a['id'] <=> $b['id'];
        });

        $typedResult = $scenario['dataCasting'] || $scenario['srcTyped'];
        if ($typedResult) {
            $this->assertEquals(
                [
                    ['id' => 1, 'text' => 'alpha', $flagName => true],
                    ['id' => 2, 'text' => 'beta',  $flagName => false],
                    ['id' => 3, 'text' => 'gamma', $flagName => true],
                ],
                $data,
            );
        }
        if (!$typedResult) {
            $this->assertEquals(
                [
                    ['id' => '1', 'text' => 'alpha', $flagName => 'true'],
                    ['id' => '2', 'text' => 'beta',  $flagName => 'false'],
                    ['id' => '3', 'text' => 'gamma', $flagName => 'true'],
                ],
                $data,
            );
        }

        // source is string type
        if (!$scenario['srcTyped']) {
            $dataValues = [
                BigqueryQuote::quote('4'),
                BigqueryQuote::quote('keep'),
                BigqueryQuote::quote('false'),
            ];
        } else {
            $dataValues =
                [
                    4,
                    BigqueryQuote::quote('keep'),
                    'false',
                ];
        }
        // insert new row
        $sql = sprintf(
            'INSERT INTO %s.%s (id, text, flag) VALUES (%s, %s, %s)',
            BigqueryQuote::quoteSingleIdentifier($dataset),
            BigqueryQuote::quoteSingleIdentifier($sourceTable),
            ...$dataValues,
        );
        $bq->runQuery($bq->query($sql));

        $response = $handler($this->projectCredentials, $cmd, [], new RuntimeOptions(['runId' => $this->testRunId]));

        // TODO?
//        if ($scenario['withTimestamp']) {
//            $this->assertTimestamp($bq, $dataset, $destTable);
//        }
        $data = $this->fetchTable($bq, $dataset, $destTable, ['id', 'text', $flagName]);
        usort($data, function ($a, $b) {
            return $a['id'] <=> $b['id'];
        });
        if ($typedResult) {
            if (!$pkSet) {
                $expected = [
                    ['id' => 1, 'text' => 'alpha', 'flag' => true],
                    ['id' => 1, 'text' => 'alpha', 'flag' => true],
                    ['id' => 2, 'text' => 'beta', 'flag' => false],
                    ['id' => 2, 'text' => 'beta', 'flag' => false],
                    ['id' => 3, 'text' => 'gamma', 'flag' => true],
                    ['id' => 3, 'text' => 'gamma', 'flag' => true],
                    ['id' => 4, 'text' => 'keep', 'flag' => false],
                ];
            } else {
                $expected = [
                    ['id' => 1, 'text' => 'alpha', 'flag' => true],
                    ['id' => 2, 'text' => 'beta', 'flag' => false],
                    ['id' => 3, 'text' => 'gamma', 'flag' => true],
                    ['id' => 4, 'text' => 'keep', 'flag' => false],
                ];
            }
            $this->assertEquals(
                $expected,
                $data,
            );
        }
        if (!$typedResult) {
            if (!$pkSet) {
                $expected = [
                    ['id' => '1', 'text' => 'alpha', 'flag' => 'true'],
                    ['id' => '1', 'text' => 'alpha', 'flag' => 'true'],
                    ['id' => '2', 'text' => 'beta', 'flag' => 'false'],
                    ['id' => '2', 'text' => 'beta', 'flag' => 'false'],
                    ['id' => '3', 'text' => 'gamma', 'flag' => 'true'],
                    ['id' => '3', 'text' => 'gamma', 'flag' => 'true'],
                    ['id' => '4', 'text' => 'keep', 'flag' => 'false'],
                ];
            } else {
                $expected = [
                    ['id' => '1', 'text' => 'alpha', 'flag' => 'true'],
                    ['id' => '2', 'text' => 'beta', 'flag' => 'false'],
                    ['id' => '3', 'text' => 'gamma', 'flag' => 'true'],
                    ['id' => '4', 'text' => 'keep', 'flag' => 'false'],
                ];
            }

            $this->assertEquals(
                $expected,
                $data,
            );
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

    private function insertSourceRows(BigQueryClient $bq, string $dataset, string $table, bool $srcTyped): void
    {
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
