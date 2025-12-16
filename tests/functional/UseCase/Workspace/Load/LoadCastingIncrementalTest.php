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
        $directions = ['string_to_typed', 'typed_to_string'];
        $withTimestamps = [true, false];
        $dedupTypes = [ImportOptions\DedupType::INSERT_DUPLICATES, ImportOptions\DedupType::UPDATE_DUPLICATES];

        foreach ($directions as $direction) {
            foreach ($withTimestamps as $withTs) {
                foreach ($dedupTypes as $dedupType) {
                    $sourceSchemaType = $direction === 'string_to_typed' ? 'string' : 'typed';
                    $destinationSchemaType = $direction === 'string_to_typed' ? 'typed' : 'string';
                    $importStrategy = $destinationSchemaType === 'typed'
                        ? ImportStrategy::USER_DEFINED_TABLE
                        : ImportStrategy::STRING_TABLE;

                    $requireDedupColumns = ($dedupType === ImportOptions\DedupType::UPDATE_DUPLICATES);

                    $shouldPass = true;
                    $reasonIfFail = null;
                    if ($direction === 'string_to_typed' && $dedupType === ImportOptions\DedupType::UPDATE_DUPLICATES) {
                        $shouldPass = false;
                        $reasonIfFail = 'INC+UPDATE validates destination vs expected (string->typed mismatch).';
                    }

                    yield sprintf(
                        '%s|ts:%s|dedup:%s',
                        $direction,
                        $withTs ? 'Y' : 'N',
                        $dedupType === ImportOptions\DedupType::INSERT_DUPLICATES ? 'INSERT' : 'UPDATE',
                    ) => [
                        [
                            'withTimestamp' => $withTs,
                            'dedupType' => $dedupType,
                            'direction' => $direction,
                            'importStrategy' => $importStrategy,
                            'sourceSchemaType' => $sourceSchemaType,
                            'destinationSchemaType' => $destinationSchemaType,
                            'requireDedupColumns' => $requireDedupColumns,
                            'shouldPass' => $shouldPass,
                            'reasonIfFail' => $reasonIfFail,
                            'verifyCastedTypes' => ($direction === 'string_to_typed'),
                            'verifyStringsOnly' => ($direction === 'typed_to_string'),
                        ],
                    ];
                }
            }
        }
    }

    /**
     * @dataProvider incrementalProvider
     * @param array{
     *       withTimestamp: bool,
     *       dedupType: ImportOptions\DedupType::*,
     *       direction: string,
     *       importStrategy: ImportStrategy::*,
     *       sourceSchemaType: string,
     *       destinationSchemaType: string,
     *       requireDedupColumns: bool,
     *       shouldPass: bool,
     *       reasonIfFail: string|null,
     *       verifyCastedTypes: bool,
     *       verifyStringsOnly: bool
     *   } $scenario
     */
    public function testIncrementalLoadCasting(array $scenario): void
    {
        $dataset = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTable = $this->getTestHash() . '_incr_src';
        $destTable = $this->getTestHash() . '_incr_dest';

        $bq = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $qb = new BigqueryTableQueryBuilder();

        // Cleanup
        $this->dropTableIfExists($bq, $dataset, $sourceTable);
        $this->dropTableIfExists($bq, $dataset, $destTable);

        // Source (no PKs on BQ side, dedup works logically)
        $srcDef = $this->createSourceDefinition($dataset, $sourceTable, $scenario['sourceSchemaType']);
        $bq->runQuery($bq->query($qb->getCreateTableCommand(
            $srcDef->getSchemaName(),
            $srcDef->getTableName(),
            $srcDef->getColumnsDefinitions(),
            [],
        )));
        $this->insertSourceRows($bq, $dataset, $sourceTable, $scenario['sourceSchemaType']);

        // Destination (add _timestamp when requested)
        $destDef = $this->createDestinationDefinition(
            $dataset,
            $destTable,
            $scenario['destinationSchemaType'],
            $scenario['withTimestamp'],
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
        foreach (['id', 'text', 'flag'] as $col) {
            $mappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
                ->setSourceColumnName($col)
                ->setDestinationColumnName($col);
        }

        $cmd = (new LoadTableToWorkspaceCommand())
            ->setSource(
                (new LoadTableToWorkspaceCommand\SourceTableMapping())
                    ->setPath($path)
                    ->setTableName($sourceTable)
                    ->setColumnMappings($mappings),
            )
            ->setDestination((new Table())->setPath($path)->setTableName($destTable));

        $options = (new ImportOptions())
            ->setImportType(ImportOptions\ImportType::INCREMENTAL)
            ->setDedupType($scenario['dedupType'])
            ->setImportStrategy($scenario['importStrategy'])
            ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
            ->setNumberOfIgnoredLines(0);
        if ($scenario['withTimestamp']) {
            $options->setTimestampColumn('_timestamp');
        }
        if ($scenario['requireDedupColumns']) {
            $dedup = new RepeatedField(GPBType::STRING);
            $dedup[] = 'id';
            $options->setDedupColumnsNames($dedup);
        }
        $cmd->setImportOptions($options);

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        if ($scenario['shouldPass'] === false) {
            try {
                $handler($this->projectCredentials, $cmd, [], new RuntimeOptions(['runId' => $this->testRunId]));
                $this->fail('Expected scenario to fail: ' . ($scenario['reasonIfFail'] ?? ''));
            } catch (DriverColumnsMismatchException|BadRequestException $e) {
                $this->assertTrue(true, 'Failure as expected: ' . $e->getMessage());
            }
            return;
        }

        $response = $handler($this->projectCredentials, $cmd, [], new RuntimeOptions(['runId' => $this->testRunId]));

        if ($scenario['withTimestamp']) {
            $this->assertTimestamp($bq, $dataset, $destTable);
        }

        if ($scenario['verifyCastedTypes'] && $scenario['destinationSchemaType'] === 'typed') {
            $data = $this->fetchTable($bq, $dataset, $destTable, ['id', 'text', 'flag']);
            usort($data, function ($a, $b) {
                return $a['id'] <=> $b['id'];
            });
            $this->assertEquals(
                [
                    ['id' => 1, 'text' => 'alpha', 'flag' => true],
                    ['id' => 2, 'text' => 'beta', 'flag' => false],
                    ['id' => 3, 'text' => 'gamma', 'flag' => true],
                ],
                $data,
            );
        }
        if ($scenario['verifyStringsOnly'] && $scenario['destinationSchemaType'] === 'string') {
            $data = $this->fetchTable($bq, $dataset, $destTable, ['id', 'text', 'flag']);
            usort($data, function ($a, $b) {
                return $a['id'] <=> $b['id'];
            });
            $this->assertEquals(
                [
                    ['id' => '1', 'text' => 'alpha', 'flag' => 'true'],
                    ['id' => '2', 'text' => 'beta', 'flag' => 'false'],
                    ['id' => '3', 'text' => 'gamma', 'flag' => 'true'],
                ],
                $data,
            );
        }

        // source is string type
        if ($scenario['verifyCastedTypes']) {
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

        if ($scenario['withTimestamp']) {
            $this->assertTimestamp($bq, $dataset, $destTable);
        }

        if ($scenario['verifyCastedTypes'] && $scenario['destinationSchemaType'] === 'typed') {
            $data = $this->fetchTable($bq, $dataset, $destTable, ['id', 'text', 'flag']);
            usort($data, function ($a, $b) {
                return $a['id'] <=> $b['id'];
            });
            if ($scenario['dedupType'] === ImportOptions\DedupType::INSERT_DUPLICATES) {
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
        if ($scenario['verifyStringsOnly'] && $scenario['destinationSchemaType'] === 'string') {
            $data = $this->fetchTable($bq, $dataset, $destTable, ['id', 'text', 'flag']);
            usort($data, function ($a, $b) {
                return $a['id'] <=> $b['id'];
            });
            if ($scenario['dedupType'] === ImportOptions\DedupType::INSERT_DUPLICATES) {
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

    private function createSourceDefinition(string $dataset, string $table, string $schemaType): BigqueryTableDefinition
    {
        if ($schemaType === 'string') {
            return new BigqueryTableDefinition(
                $dataset,
                $table,
                false,
                new ColumnCollection([
                    BigqueryColumn::createGenericColumn('id'),
                    BigqueryColumn::createGenericColumn('text'),
                    BigqueryColumn::createGenericColumn('flag'),
                ]),
                [],
            );
        }
        return new BigqueryTableDefinition(
            $dataset,
            $table,
            false,
            new ColumnCollection([
                new BigqueryColumn('id', new BigqueryType(BigqueryType::TYPE_INT, [])),
                new BigqueryColumn('text', new BigqueryType(BigqueryType::TYPE_STRING, [])),
                new BigqueryColumn('flag', new BigqueryType(BigqueryType::TYPE_BOOLEAN, [])),
            ]),
            [],
        );
    }

    private function createDestinationDefinition(
        string $dataset,
        string $table,
        string $schemaType,
        bool $withTimestamp,
    ): BigqueryTableDefinition {
        $columns = [];
        if ($schemaType === 'string') {
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
            [],
        );
    }

    private function insertSourceRows(BigQueryClient $bq, string $dataset, string $table, string $schemaType): void
    {
        if ($schemaType === 'string') {
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
