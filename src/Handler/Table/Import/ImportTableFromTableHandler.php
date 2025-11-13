<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Cloud\Core\Exception\ConflictException;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery as BigqueryDefinition;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\Assert;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryInputDataException;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\Exception\ColumnsMismatchException;
use Keboola\Db\ImportExport\Storage\Bigquery\SelectSource;
use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\CreateImportOptionHelper;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\BigQuery\Handler\Table\BadExportFilterParametersException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ColumnsMismatchException as DriverColumnsMismatchException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromTableLib\CopyImportFromTableToTable;
use Keboola\StorageDriver\BigQuery\Handler\Table\ObjectAlreadyExistsException;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\BigQuery\QueryBuilder\TableImportQueryBuilder;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table as CommandDestination;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\Import\ImportValidationException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use LogicException;
use Throwable;

final class ImportTableFromTableHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param TableImportFromTableCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof TableImportFromTableCommand);

        assert($runtimeOptions->getMeta() === null);

        // validate
        $sourceMapping = $command->getSource();
        assert($sourceMapping !== null, 'TableImportFromFileCommand.source is required.');
        $destination = $command->getDestination();
        assert($destination !== null, 'TableImportFromFileCommand.destination is required.');
        $importOptions = $command->getImportOptions();
        assert($importOptions !== null, 'TableImportFromFileCommand.importOptions is required.');

        /** @var array<string, string> $queryTags */
        $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

        $bqClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            $queryTags,
        );

        [$source, $sourceTableDefinition, $fullSourceTableDefinition] = $this->createSource($bqClient, $command);
        $bigqueryImportOptions = CreateImportOptionHelper::createOptions($importOptions, $features);
        $expectedDestinationColumns = $this->buildDestinationColumnsFromMapping(
            $sourceTableDefinition,
            $sourceMapping,
        );
        $destinationDefinition = $this->resolveDestinationDefinition(
            $bqClient,
            $destination,
            $importOptions,
            $expectedDestinationColumns,
        );

        if ($importOptions->getImportType() === ImportType::INCREMENTAL
            && $importOptions->getDedupType() === ImportOptions\DedupType::UPDATE_DUPLICATES
        ) {
            $this->validateIncrementalDestinationTable(
                $destinationDefinition,
                $expectedDestinationColumns,
                $fullSourceTableDefinition,
            );
        }

        // Replace is only available in view or clone import
        $shouldDropTableIfExists = $importOptions->getCreateMode() === ImportOptions\CreateMode::REPLACE
            && in_array($importOptions->getImportType(), [ImportType::VIEW, ImportType::PBCLONE], true);

        if ($shouldDropTableIfExists) {
            $dataset = $bqClient->dataset(ProtobufHelper::repeatedStringToArray($destination->getPath())[0]);
            $table = $dataset->table($destination->getTableName());
            if ($table->exists()) {
                $table->delete();
            }
        }

        switch ($importOptions->getImportType()) {
            case ImportType::FULL:
            case ImportType::INCREMENTAL:
                $importResult = $this->importByTableCopy(
                    $bqClient,
                    $destination,
                    $destinationDefinition,
                    $importOptions,
                    $source,
                    $sourceTableDefinition,
                    $bigqueryImportOptions,
                    $sourceMapping,
                );
                break;
            case ImportType::VIEW:
                assert($source instanceof Table);
                $importResult = $this->createView(
                    $bqClient,
                    $destination,
                    $source,
                );
                break;
            case ImportType::PBCLONE:
                assert($source instanceof Table);
                $importResult = $this->clone(
                    $bqClient,
                    $destination,
                    $source,
                );
                break;
            default:
                throw new LogicException(sprintf(
                    'Unknown import type "%s".',
                    $importOptions->getImportType(),
                ));
        }

        $response = new TableImportResponse();
        $destinationRef = new BigqueryTableReflection(
            $bqClient,
            ProtobufHelper::repeatedStringToArray($destination->getPath())[0],
            $destination->getTableName(),
        );
        $destinationStats = $destinationRef->getTableStats();
        $response->setTableRowsCount($destinationStats->getRowsCount());
        $response->setTableSizeBytes($destinationStats->getDataSizeBytes());
        $response->setImportedColumns(ProtobufHelper::arrayToRepeatedString($importResult->getImportedColumns()));
        $response->setImportedRowsCount($importResult->getImportedRowsCount());
        $timers = new RepeatedField(GPBType::MESSAGE, TableImportResponse\Timer::class);
        foreach ($importResult->getTimers() as $timerArr) {
            $timer = new TableImportResponse\Timer();
            $timer->setName($timerArr['name']);
            $timer->setDuration($timerArr['durationSeconds']);
            $timers[] = $timer;
        }
        $response->setTimers($timers);

        return $response;
    }

    /**
     * @return array{0: SqlSourceInterface, 1: BigqueryTableDefinition, 2: BigqueryTableDefinition}
     */
    private function createSource(
        BigQueryClient $bqClient,
        TableImportFromTableCommand $command,
    ): array {
        $sourceMapping = $command->getSource();
        assert($sourceMapping !== null);
        $sourceDataset = ProtobufHelper::repeatedStringToArray($sourceMapping->getPath());
        assert(isset($sourceDataset[0]), 'TableImportFromTableCommand.source.path is required.');

        $sourceTableDefinition = (new BigqueryTableReflection(
            $bqClient,
            $sourceDataset[0],
            $sourceMapping->getTableName(),
        ))->getTableDefinition();
        assert($sourceTableDefinition instanceof BigqueryTableDefinition);

        $sourceColumns = $this->getSourceColumns($sourceMapping, $sourceTableDefinition);
        $effectiveSourceDefinition = $this->filterSourceDefinition(
            $sourceTableDefinition,
            $sourceColumns,
        );
        $isFullColumnSet = $this->isFullColumnSet($sourceColumns, $sourceTableDefinition);
        if ($this->shouldUseSelectSource($sourceMapping, $isFullColumnSet)) {
            // For SELECT queries with WHERE filters, we need to include WHERE filter columns
            // in the definition for validation, but NOT in the actual SELECT list
            $whereFilterColumns = $this->getWhereFilterColumns($sourceMapping);
            $allColumnsForValidation = array_unique(array_merge($sourceColumns, $whereFilterColumns));
            $definitionForQuery = $this->filterSourceDefinition(
                $sourceTableDefinition,
                $allColumnsForValidation,
            );

            $queryBuilder = new TableImportQueryBuilder($bqClient, new ColumnConverter());
            $queryResponse = $queryBuilder->buildSelectSourceSql(
                $definitionForQuery,
                $sourceColumns,
                $sourceMapping,
            );
            $source = new SelectSource(
                $queryResponse->getQuery(),
                // @phpstan-ignore-next-line argument.type incompatible types in library for PHPStan
                $queryResponse->getBindings(),
                $sourceColumns,
                [],
                $effectiveSourceDefinition->getPrimaryKeysNames(),
            );
        } else {
            $source = new Table(
                $effectiveSourceDefinition->getSchemaName(),
                $effectiveSourceDefinition->getTableName(),
                $sourceColumns,
                $effectiveSourceDefinition->getPrimaryKeysNames(),
            );
        }

        return [$source, $effectiveSourceDefinition, $sourceTableDefinition];
    }

    /**
     * @return string[]
     */
    private function getSourceColumns(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
        BigqueryTableDefinition $sourceTableDefinition,
    ): array {
        $columns = [];
        $columnMappingsField = $sourceMapping->getColumnMappings();
        if ($columnMappingsField !== null) {
            /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping $mapping */
            foreach ($columnMappingsField->getIterator() as $mapping) {
                $columns[] = $mapping->getSourceColumnName();
            }
        }
        if ($columns !== []) {
            return $columns;
        }

        $definitionColumns = [];
        /** @var BigqueryColumn $column */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $column) {
            $definitionColumns[] = $column->getColumnName();
        }

        return $definitionColumns;
    }

    /**
     * @return string[]
     */
    private function getWhereFilterColumns(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ): array {
        $columns = [];
        $whereFilters = $sourceMapping->getWhereFilters();
        if ($whereFilters !== null && $whereFilters->count() > 0) {
            /** @var \Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter $filter */
            foreach ($whereFilters as $filter) {
                $columns[] = $filter->getColumnsName();
            }
        }
        return $columns;
    }

    /**
     * @param string[] $columns
     */
    private function filterSourceDefinition(
        BigqueryTableDefinition $sourceDefinition,
        array $columns,
    ): BigqueryTableDefinition {
        if ($columns === []) {
            return $sourceDefinition;
        }

        $columnMap = [];
        /** @var BigqueryColumn $column */
        foreach ($sourceDefinition->getColumnsDefinitions() as $column) {
            $columnMap[strtolower($column->getColumnName())] = $column;
        }

        $filtered = [];
        foreach ($columns as $columnName) {
            $column = $columnMap[strtolower($columnName)] ?? null;
            if ($column === null) {
                throw new DriverColumnsMismatchException(sprintf(
                    'Column "%s" not found in source table %s.%s.',
                    $columnName,
                    $sourceDefinition->getSchemaName(),
                    $sourceDefinition->getTableName(),
                ));
            }
            $filtered[] = $column;
        }

        return new BigqueryTableDefinition(
            $sourceDefinition->getSchemaName(),
            $sourceDefinition->getTableName(),
            $sourceDefinition->isTemporary(),
            new ColumnCollection($filtered),
            $sourceDefinition->getPrimaryKeysNames(),
        );
    }

    /**
     * @param string[] $columns
     */
    private function isFullColumnSet(
        array $columns,
        BigqueryTableDefinition $sourceDefinition,
    ): bool {
        if ($columns === []) {
            return true;
        }

        $sourceColumns = [];
        /** @var BigqueryColumn $column */
        foreach ($sourceDefinition->getColumnsDefinitions() as $column) {
            $sourceColumns[] = strtolower($column->getColumnName());
        }

        if (count($columns) !== count($sourceColumns)) {
            return false;
        }

        foreach ($columns as $index => $columnName) {
            if ($sourceColumns[$index] !== strtolower($columnName)) {
                return false;
            }
        }

        return true;
    }

    private function shouldUseSelectSource(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
        bool $isFullColumnSet,
    ): bool {
        $whereFilters = $sourceMapping->getWhereFilters();
        $hasWhereFilters = $whereFilters !== null && $whereFilters->count() > 0;
        return !$isFullColumnSet
            || (int) $sourceMapping->getSeconds() > 0
            || (int) $sourceMapping->getLimit() > 0
            || $hasWhereFilters;
    }

    /**
     * @return array{0: BigqueryTableDefinition|null, 1: Result}
     */
    private function import(
        BigQueryClient $bqClient,
        BigqueryTableDefinition $destinationDefinition,
        ImportOptions $options,
        SqlSourceInterface $source,
        BigqueryTableDefinition $sourceTableDefinition,
        BigqueryImportOptions $importOptions,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ): array {
        $dedupColumns = ProtobufHelper::repeatedStringToArray($options->getDedupColumnsNames());
        if ($options->getDedupType() === ImportOptions\DedupType::UPDATE_DUPLICATES && count($dedupColumns) !== 0) {
            $destinationDefinition = new BigqueryTableDefinition(
                $destinationDefinition->getSchemaName(),
                $destinationDefinition->getTableName(),
                $destinationDefinition->isTemporary(),
                $destinationDefinition->getColumnsDefinitions(),
                $dedupColumns, // add dedup columns separately as BQ has no primary keys
            );
        }

        $isFullImport = $options->getImportType() === ImportType::FULL;
        $insertOnlyDuplicates = $options->getDedupType() === ImportOptions\DedupType::INSERT_DUPLICATES;
        if ($isFullImport && $insertOnlyDuplicates && !$importOptions->useTimestamp()) {
            // when full load is performed with no deduplication only copy data using ToStage class
            // this will skip moving data to stage table
            // this is used on full load into workspace where data are deduplicated already
            $toStageImporter = new ToStageImporter($bqClient);
            try {
                $importState = $toStageImporter->importToStagingTable(
                    $source,
                    $destinationDefinition,
                    $importOptions,
                );
            } catch (ColumnsMismatchException $e) {
                throw new DriverColumnsMismatchException($e->getMessage());
            }
            return [null, $importState->getResult()];
        }

        $columnMappingsField = $sourceMapping->getColumnMappings();
        /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping[] $mappings */
        $mappings = iterator_to_array($columnMappingsField->getIterator());
        // load to staging table
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinitionWithMapping(
            $destinationDefinition,
            $mappings,
        );

        $isColumnIdentical = true;
        try {
            Assert::assertSameColumnsOrdered(
                $sourceTableDefinition->getColumnsDefinitions(),
                $stagingTable->getColumnsDefinitions(),
                [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            );
        } catch (ColumnsMismatchException) {
            $isColumnIdentical = false;
        }

        if ($isColumnIdentical && $source instanceof Table) {
            // if columns are identical we can use COPY statement to make import faster
            $toStageImporter = new CopyImportFromTableToTable($bqClient);
        } else {
            $bqClient->runQuery($bqClient->query(
                (new BigqueryTableQueryBuilder())->getCreateTableCommand(
                    $stagingTable->getSchemaName(),
                    $stagingTable->getTableName(),
                    $stagingTable->getColumnsDefinitions(),
                    [],
                ),
            ));
            // load to staging table
            $toStageImporter = new ToStageImporter($bqClient);
        }
        try {
            $importState = $toStageImporter->importToStagingTable(
                $source,
                $stagingTable,
                $importOptions,
            );
            // import data to destination
            $toFinalTableImporter = new FullImporter($bqClient);
            if ($importOptions->isIncremental()) {
                $toFinalTableImporter = new IncrementalImporter($bqClient);
            }
            $importResult = $toFinalTableImporter->importToTable(
                $stagingTable,
                $destinationDefinition,
                $importOptions,
                $importState,
            );
        } catch (ColumnsMismatchException $e) {
            throw new DriverColumnsMismatchException($e->getMessage());
        } catch (BigqueryException $e) {
            BadExportFilterParametersException::handleWrongTypeInFilters($e);
            throw $e;
        }
        return [$stagingTable, $importResult];
    }

    private function buildDestinationColumnsFromMapping(
        BigqueryTableDefinition $sourceTableDefinition,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ): ColumnCollection {
        $sourceColumns = [];
        /** @var BigqueryColumn $column */
        foreach ($sourceTableDefinition->getColumnsDefinitions() as $column) {
            $sourceColumns[strtolower($column->getColumnName())] = $column;
        }

        $definitions = [];
        $columnMappingsField = $sourceMapping->getColumnMappings();
        /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping[] $mappings */
        $mappings = iterator_to_array($columnMappingsField->getIterator());
        if ($mappings === []) {
            /** @var BigqueryColumn $column */
            foreach ($sourceTableDefinition->getColumnsDefinitions() as $column) {
                $definitions[] = $this->cloneColumnWithName($column, $column->getColumnName());
            }
            return new ColumnCollection($definitions);
        }

        $missingColumns = [];
        /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping $mapping */
        foreach ($mappings as $mapping) {
            $sourceColumn = $sourceColumns[strtolower($mapping->getSourceColumnName())] ?? null;
            if ($sourceColumn === null) {
                $missingColumns[] = $mapping->getSourceColumnName();
                continue;
            }
            $definitions[] = $this->cloneColumnWithName($sourceColumn, $mapping->getDestinationColumnName());
        }

        if ($missingColumns !== []) {
            throw new DriverColumnsMismatchException(sprintf(
                'Some columns are missing in source table %s. Missing columns: %s',
                sprintf('%s.%s', $sourceTableDefinition->getSchemaName(), $sourceTableDefinition->getTableName()),
                implode(',', $missingColumns),
            ));
        }

        return new ColumnCollection($definitions);
    }

    private function cloneColumnWithName(BigqueryColumn $column, string $newName): BigqueryColumn
    {
        /** @var BigqueryDefinition $definition */
        $definition = $column->getColumnDefinition();
        $options = [
            'length' => $definition->getLength(),
            'nullable' => $definition->isNullable(),
            'default' => $definition->getDefault(),
        ];
        if (method_exists($definition, 'getFieldAsArray') && $definition->getFieldAsArray() !== null) {
            $options['fieldAsArray'] = $definition->getFieldAsArray();
        }

        return new BigqueryColumn(
            $newName,
            new BigqueryDefinition(
                $definition->getType(),
                $options,
            ),
        );
    }

    private function resolveDestinationDefinition(
        BigQueryClient $bqClient,
        CommandDestination $destination,
        ImportOptions $importOptions,
        ColumnCollection $expectedColumns,
    ): BigqueryTableDefinition {
        $path = ProtobufHelper::repeatedStringToArray($destination->getPath());
        assert(isset($path[0]), 'TableImportFromTableCommand.destination.path is required.');
        $schemaName = $path[0];
        $tableName = $destination->getTableName();

        $reflection = new BigqueryTableReflection($bqClient, $schemaName, $tableName);
        try {
            $tableDefinition = $reflection->getTableDefinition();
            assert($tableDefinition instanceof BigqueryTableDefinition);
            return $tableDefinition;
        } catch (TableNotExistsReflectionException $e) {
            $this->createDestinationTable(
                $bqClient,
                $schemaName,
                $tableName,
                $expectedColumns,
                ProtobufHelper::repeatedStringToArray($importOptions->getDedupColumnsNames()),
            );
            return new BigqueryTableDefinition(
                $schemaName,
                $tableName,
                false,
                $expectedColumns,
                ProtobufHelper::repeatedStringToArray($importOptions->getDedupColumnsNames()),
            );
        }
    }

    /**
     * @param string[] $primaryKeys
     */
    private function createDestinationTable(
        BigQueryClient $bqClient,
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys,
    ): void {
        $sql = (new BigqueryTableQueryBuilder())->getCreateTableCommand(
            $schemaName,
            $tableName,
            $columns,
            $primaryKeys,
        );
        $bqClient->runQuery($bqClient->query($sql));
    }

    private function validateIncrementalDestinationTable(
        BigqueryTableDefinition $destinationDefinition,
        ColumnCollection $expectedColumns,
        BigqueryTableDefinition $sourceDefinition,
    ): void {
        $actualColumns = $this->normalizeColumnsByName($destinationDefinition->getColumnsDefinitions());
        $expectedColumnsNormalized = $this->normalizeColumnsByName($expectedColumns);

        // Exclude system columns like _timestamp from validation as they're auto-managed
        $systemColumns = ['_timestamp'];
        $actualColumnsFiltered = array_filter(
            $actualColumns,
            fn($key) => !in_array($key, $systemColumns, true),
            ARRAY_FILTER_USE_KEY,
        );

        $missingInSource = array_values(
            array_diff(
                array_keys($actualColumnsFiltered),
                array_keys($expectedColumnsNormalized),
            ),
        );
        if ($missingInSource !== []) {
            throw new DriverColumnsMismatchException(sprintf(
                'Some columns are missing in source table %s. Missing columns: %s',
                sprintf('%s.%s', $sourceDefinition->getSchemaName(), $sourceDefinition->getTableName()),
                implode(',', array_map(
                    fn(string $key) => $actualColumns[$key]->getColumnName(),
                    $missingInSource,
                )),
            ));
        }

        $missingInWorkspace = array_values(
            array_diff(
                array_keys($expectedColumnsNormalized),
                array_keys($actualColumns),
            ),
        );
        if ($missingInWorkspace !== []) {
            throw new DriverColumnsMismatchException(sprintf(
                'Some columns are missing in workspace table %s. Missing columns: %s',
                sprintf('%s.%s', $destinationDefinition->getSchemaName(), $destinationDefinition->getTableName()),
                implode(',', array_map(
                    fn(string $key) => $expectedColumnsNormalized[$key]->getColumnName(),
                    $missingInWorkspace,
                )),
            ));
        }

        $definitionErrors = [];
        foreach ($expectedColumnsNormalized as $name => $expectedColumn) {
            $actualColumn = $actualColumns[$name];
            if (!$this->columnDefinitionsMatch($expectedColumn, $actualColumn)) {
                /** @var BigqueryDefinition $expectedDef */
                $expectedDef = $expectedColumn->getColumnDefinition();
                /** @var BigqueryDefinition $actualDef */
                $actualDef = $actualColumn->getColumnDefinition();
                $definitionErrors[] = sprintf(
                    '\'%s\' mapping \'%s\' / \'%s\'',
                    $actualColumn->getColumnName(),
                    $expectedDef->getSQLDefinition(),
                    $actualDef->getSQLDefinition(),
                );
            }
        }

        if ($definitionErrors !== []) {
            throw new DriverColumnsMismatchException(sprintf(
                'Column definitions mismatch. Details: %s',
                implode('; ', $definitionErrors),
            ));
        }
    }

    /**
     * @return array<string, BigqueryColumn>
     */
    private function normalizeColumnsByName(ColumnCollection $columns): array
    {
        $normalized = [];
        /** @var BigqueryColumn $column */
        foreach ($columns as $column) {
            $normalized[strtolower($column->getColumnName())] = $column;
        }

        return $normalized;
    }

    private function columnDefinitionsMatch(BigqueryColumn $expected, BigqueryColumn $actual): bool
    {
        /** @var BigqueryDefinition $expectedDef */
        $expectedDef = $expected->getColumnDefinition();
        /** @var BigqueryDefinition $actualDef */
        $actualDef = $actual->getColumnDefinition();

        // For workspace string tables, destination columns are always STRING type
        // regardless of source type, so allow any type -> STRING conversion
        // Also be lenient with nullability and length for string tables
        if (strcasecmp($actualDef->getType(), BigqueryDefinition::TYPE_STRING) === 0) {
            return true;
        }

        // For typed tables, require exact type match
        if (strcasecmp($expectedDef->getType(), $actualDef->getType()) !== 0) {
            return false;
        }

        if ($expectedDef->isNullable() !== $actualDef->isNullable()) {
            return false;
        }

        $expectedLength = (string) ($expectedDef->getLength() ?? '');
        $actualLength = (string) ($actualDef->getLength() ?? '');

        return $expectedLength === $actualLength;
    }

    private function importByTableCopy(
        BigQueryClient $bqClient,
        CommandDestination $destination,
        BigqueryTableDefinition $destinationDefinition,
        ImportOptions $importOptions,
        SqlSourceInterface $source,
        BigqueryTableDefinition $sourceTableDefinition,
        BigqueryImportOptions $bigqueryImportOptions,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ): Result {
        $stagingTable = null;
        try {
            [
                $stagingTable,
                $importResult,
            ] = $this->import(
                $bqClient,
                $destinationDefinition,
                $importOptions,
                $source,
                $sourceTableDefinition,
                $bigqueryImportOptions,
                $sourceMapping,
            );
        } catch (BigqueryInputDataException $e) {
            throw new ImportValidationException(DecodeErrorMessage::getErrorMessage($e));
        } catch (BigqueryException $e) {
            throw MaximumLengthOverflowException::handleException($e);
        } finally {
            if ($stagingTable !== null) {
                try {
                    $bqClient->runQuery($bqClient->query(
                        (new BigqueryTableQueryBuilder())->getDropTableCommand(
                            $stagingTable->getSchemaName(),
                            $stagingTable->getTableName(),
                        ),
                    ));
                } catch (Throwable $e) {
                    // ignore
                }
            }
        }
        return $importResult;
    }

    /**
     * @throws ObjectAlreadyExistsException
     * @throws ConflictException
     */
    private function createView(
        BigQueryClient $bqClient,
        CommandDestination $destination,
        Table $source,
    ): Result {
        $sql = sprintf(
            <<<SQL
CREATE VIEW %s.%s AS (
  SELECT
    *
  FROM
    %s.%s
);
SQL,
            BigqueryQuote::quoteSingleIdentifier(ProtobufHelper::repeatedStringToArray($destination->getPath())[0]),
            BigqueryQuote::quoteSingleIdentifier($destination->getTableName()),
            BigqueryQuote::quoteSingleIdentifier($source->getSchema()),
            BigqueryQuote::quoteSingleIdentifier($source->getTableName()),
        );

        try {
            $bqClient->runQuery(
                $bqClient->query($sql),
            );
        } catch (ConflictException $e) {
            throw ObjectAlreadyExistsException::handleConflictException($e);
        }

        return new Result([
            'importedRowsCount' => 0,
        ]);
    }

    /**
     * @throws ConflictException
     * @throws BadRequestException
     * @throws ObjectAlreadyExistsException
     */
    private function clone(BigQueryClient $bqClient, CommandDestination $destination, Table $source): Result
    {
        $sql = sprintf(
            <<<SQL
CREATE TABLE %s.%s CLONE %s.%s;
SQL,
            BigqueryQuote::quoteSingleIdentifier(ProtobufHelper::repeatedStringToArray($destination->getPath())[0]),
            BigqueryQuote::quoteSingleIdentifier($destination->getTableName()),
            BigqueryQuote::quoteSingleIdentifier($source->getSchema()),
            BigqueryQuote::quoteSingleIdentifier($source->getTableName()),
        );

        try {
            $bqClient->runQuery(
                $bqClient->query($sql),
            );
            return new Result([
                'importedRowsCount' => 0,
            ]);
        } catch (ConflictException $e) {
            throw ObjectAlreadyExistsException::handleConflictException($e);
        } catch (BadRequestException $e) {
            if (str_contains($e->getMessage(), 'Cannot clone tables')) {
                return $this->cloneFallback($bqClient, $destination, $source);
            }
            throw $e;
        }
    }

    private function cloneFallback(BigQueryClient $bqClient, CommandDestination $destination, Table $source): Result
    {
        $sql = sprintf(
            <<<SQL
CREATE TABLE %s.%s AS (
  SELECT
    *
  FROM
    %s.%s
);
SQL,
            BigqueryQuote::quoteSingleIdentifier(ProtobufHelper::repeatedStringToArray($destination->getPath())[0]),
            BigqueryQuote::quoteSingleIdentifier($destination->getTableName()),
            BigqueryQuote::quoteSingleIdentifier($source->getSchema()),
            BigqueryQuote::quoteSingleIdentifier($source->getTableName()),
        );

        $bqClient->runQuery(
            $bqClient->query($sql),
        );

        return new Result([
            'importedRowsCount' => (new BigqueryTableReflection(
                $bqClient,
                ProtobufHelper::repeatedStringToArray($destination->getPath())[0],
                $destination->getTableName(),
            ))->getRowsCount(),
        ]);
    }
}
