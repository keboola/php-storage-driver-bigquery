<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Cloud\Core\Exception\ConflictException;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
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
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage\Bigquery\SelectSource;
use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\CreateImportOptionHelper;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\BigQuery\Handler\Table\BadExportFilterParametersException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ColumnsMismatchException as DriverColumnsMismatchException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromTableLib\CopyImportFromTableToTable;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\MappedTableSqlSource;
use Keboola\StorageDriver\BigQuery\Handler\Table\ObjectAlreadyExistsException;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ExportQueryBuilder;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportFilters;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
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

        [
            'table' => $source,
            'filteredSource' => $filteredSource,
        ] = $this->createSource($bqClient, $command);
        $bigqueryImportOptions = CreateImportOptionHelper::createOptions($importOptions, $features);

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
                    $importOptions,
                    $source,
                    $filteredSource,
                    $bigqueryImportOptions,
                    $sourceMapping,
                );
                break;
            case ImportType::VIEW:
                $importResult = $this->createView(
                    $bqClient,
                    $destination,
                    $source,
                );
                break;
            case ImportType::PBCLONE:
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
     * @return array{table: Table, filteredSource: SqlSourceInterface|null}
     */
    private function createSource(
        BigQueryClient $bqClient,
        TableImportFromTableCommand $command,
    ): array {
        $sourceMapping = $command->getSource();
        assert($sourceMapping !== null);
        $schemaName = ProtobufHelper::repeatedStringToArray($sourceMapping->getPath())[0];
        $tableName = $sourceMapping->getTableName();

        $sourceColumns = [];
        $columnMappings = $sourceMapping->getColumnMappings();
        if ($columnMappings !== null) {
            /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping $mapping */
            foreach ($columnMappings as $mapping) {
                $sourceColumns[] = $mapping->getSourceColumnName();
            }
        }

        $sourceTableDef = (new BigqueryTableReflection(
            $bqClient,
            $schemaName,
            $tableName,
        ))->getTableDefinition();

        $tableSource = new Table(
            $schemaName,
            $tableName,
            $sourceColumns,
            $sourceTableDef->getPrimaryKeysNames(),
        );

        $filteredSource = $this->buildFilteredSelectSource(
            $bqClient,
            $sourceMapping,
            $sourceTableDef,
            $schemaName,
            $tableName,
            $sourceColumns,
        );

        return [
            'table' => $tableSource,
            'filteredSource' => $filteredSource,
        ];
    }

    /**
     * @return array{0: BigqueryTableDefinition|null, 1: Result}
     */
    private function import(
        BigQueryClient $bqClient,
        CommandDestination $destination,
        ImportOptions $options,
        Table $source,
        BigqueryImportOptions $importOptions,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
        ?SqlSourceInterface $filteredSource,
    ): array {
        /** @var BigqueryTableDefinition $destinationDefinition */
        $destinationDefinition = (new BigqueryTableReflection(
            $bqClient,
            ProtobufHelper::repeatedStringToArray($destination->getPath())[0],
            $destination->getTableName(),
        ))->getTableDefinition();
        /** @var BigqueryTableDefinition $sourceTableDefinition */
        $sourceTableDefinition = (new BigqueryTableReflection(
            $bqClient,
            $source->getSchema(),
            $source->getTableName(),
        ))->getTableDefinition();

        $columnMappingsField = $sourceMapping->getColumnMappings();
        /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping[] $mappings */
        $mappings = $columnMappingsField !== null
            ? iterator_to_array($columnMappingsField->getIterator())
            : [];
        $baseStageSource = $filteredSource ?? $source;
        $importSource = $this->createSqlSourceFromMappings($baseStageSource, $mappings);
        $sourceForStageImport = $importSource ?? $baseStageSource;

        if ($mappings !== []) {
            $sourceTableDefinition = $this->restrictSourceTableDefinition(
                $sourceTableDefinition,
                array_map(static fn($mapping) => $mapping->getSourceColumnName(), $mappings),
            );
        }

        $importOptions = $this->synchronizeTimestampUsage($importOptions, $mappings);

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
                    $sourceForStageImport,
                    $destinationDefinition,
                    $importOptions,
                );
            } catch (ColumnsMismatchException $e) {
                throw new DriverColumnsMismatchException($e->getMessage());
            }
            return [null, $importState->getResult()];
        }
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

        $canUseCopyImporter = $filteredSource === null && $importSource === null;
        if ($isColumnIdentical && $canUseCopyImporter) {
            // if columns are identical we can use COPY statement to make import faster
            $toStageImporter = new CopyImportFromTableToTable($bqClient);
            $stageImportSource = $source;
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
            $stageImportSource = $sourceForStageImport;
        }
        try {
            $importState = $toStageImporter->importToStagingTable(
                $stageImportSource,
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

    private function buildFilteredSelectSource(
        BigQueryClient $bqClient,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
        BigqueryTableDefinition $sourceTableDefinition,
        string $schemaName,
        string $tableName,
        array $sourceColumns,
    ): ?SelectSource {
        $filters = $this->createExportFiltersFromSourceMapping($sourceMapping);
        if ($filters === null) {
            return null;
        }

        $queryBuilder = new ExportQueryBuilder($bqClient, new ColumnConverter());
        $columnsRepeated = ProtobufHelper::arrayToRepeatedString($sourceColumns);
        $orderBy = new RepeatedField(GPBType::MESSAGE, ExportOrderBy::class);

        $queryData = $queryBuilder->buildQueryFromCommand(
            ExportQueryBuilder::MODE_SELECT,
            $filters,
            $orderBy,
            $columnsRepeated,
            $sourceTableDefinition->getColumnsDefinitions(),
            $schemaName,
            $tableName,
            false,
        );

        $querySql = $queryData->getQuery();
        if ($filters->getLimit() > 0 && stripos($querySql, 'LIMIT ') === false) {
            $querySql = sprintf('%s LIMIT %d', $querySql, $filters->getLimit());
        }

        $selectedColumns = $sourceColumns === []
            ? $sourceTableDefinition->getColumnsNames()
            : $sourceColumns;

        return new SelectSource(
            $querySql,
            $queryData->getBindings(),
            $selectedColumns,
            [],
            $sourceTableDefinition->getPrimaryKeysNames(),
        );
    }

    private function createExportFiltersFromSourceMapping(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ): ?ExportFilters {
        $limit = (int) $sourceMapping->getLimit();
        $whereFilters = $sourceMapping->getWhereFilters();
        $hasWhereFilters = $whereFilters !== null && $whereFilters->count() > 0;

        $changeSince = $this->resolveChangeBoundary($sourceMapping, 'Since');
        $changeUntil = $this->resolveChangeBoundary($sourceMapping, 'Until');

        if ($limit === 0 && !$hasWhereFilters && $changeSince === '' && $changeUntil === '') {
            return null;
        }

        $filters = new ExportFilters();
        $filters->setLimit($limit);
        if ($changeSince !== '') {
            $filters->setChangeSince($changeSince);
        }
        if ($changeUntil !== '') {
            $filters->setChangeUntil($changeUntil);
        }
        if ($whereFilters !== null) {
            $filters->setWhereFilters($whereFilters);
        }

        return $filters;
    }

    private function resolveChangeBoundary(
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
        string $boundary,
    ): string {
        $method = sprintf('getChange%s', $boundary);
        if (method_exists($sourceMapping, $method)) {
            $value = (string) $sourceMapping->$method();
            if ($value !== '') {
                return $value;
            }
        }

        if ($boundary === 'Since') {
            $seconds = (int) $sourceMapping->getSeconds();
            if ($seconds > 0) {
                $timestamp = max(time() - $seconds, 0);
                return (string) $timestamp;
            }
        }

        return '';
    }

    /**
     * @param TableImportFromTableCommand\SourceTableMapping\ColumnMapping[] $mappings
     */
    private function createSqlSourceFromMappings(SqlSourceInterface $source, array $mappings): ?SqlSourceInterface
    {
        if ($mappings === []) {
            return null;
        }

        $columnMappings = [];
        foreach ($mappings as $mapping) {
            $columnMappings[] = [
                'source' => $mapping->getSourceColumnName(),
                'destination' => $mapping->getDestinationColumnName(),
            ];
        }

        if ($source instanceof Table) {
            return new MappedTableSqlSource(
                $source->getSchema(),
                $source->getTableName(),
                $columnMappings,
                $source->getPrimaryKeysNames(),
            );
        }

        return new MappedTableSqlSource(
            schema: null,
            tableName: null,
            columnMappings: $columnMappings,
            primaryKeysNames: $source->getPrimaryKeysNames(),
            baseQuery: $source->getFromStatement(),
            queryBindings: $source->getQueryBindings(),
        );
    }

    /**
     * @param array<string> $orderedColumnNames
     */
    private function restrictSourceTableDefinition(
        BigqueryTableDefinition $definition,
        array $orderedColumnNames,
    ): BigqueryTableDefinition {
        if ($orderedColumnNames === []) {
            return $definition;
        }

        $selectedColumns = [];
        foreach ($orderedColumnNames as $columnName) {
            $selectedColumns[] = $this->getSourceColumnDefinition($definition, $columnName);
        }

        return new BigqueryTableDefinition(
            $definition->getSchemaName(),
            $definition->getTableName(),
            $definition->isTemporary(),
            new ColumnCollection($selectedColumns),
            $definition->getPrimaryKeysNames(),
        );
    }

    private function getSourceColumnDefinition(
        BigqueryTableDefinition $definition,
        string $columnName,
    ): BigqueryColumn {
        /** @var BigqueryColumn $columnDefinition */
        foreach ($definition->getColumnsDefinitions() as $columnDefinition) {
            if ($columnDefinition->getColumnName() === $columnName) {
                return $columnDefinition;
            }
        }

        throw new LogicException(sprintf(
            'Column "%s" was not found in table "%s.%s".',
            $columnName,
            $definition->getSchemaName(),
            $definition->getTableName(),
        ));
    }

    /**
     * @param TableImportFromTableCommand\SourceTableMapping\ColumnMapping[] $mappings
     */
    private function synchronizeTimestampUsage(
        BigqueryImportOptions $importOptions,
        array $mappings,
    ): BigqueryImportOptions {
        if (!$importOptions->useTimestamp()) {
            return $importOptions;
        }

        $mappedDestinationColumns = array_map(
            static fn(TableImportFromTableCommand\SourceTableMapping\ColumnMapping $mapping) => $mapping->getDestinationColumnName(),
            $mappings,
        );

        if (in_array(ToStageImporterInterface::TIMESTAMP_COLUMN_NAME, $mappedDestinationColumns, true)) {
            return $importOptions;
        }

        $usingTypes = $importOptions->usingUserDefinedTypes()
            ? ImportOptionsInterface::USING_TYPES_USER
            : ImportOptionsInterface::USING_TYPES_STRING;

        return new BigqueryImportOptions(
            convertEmptyValuesToNull: $importOptions->getConvertEmptyValuesToNull(),
            isIncremental: $importOptions->isIncremental(),
            useTimestamp: false,
            numberOfIgnoredLines: $importOptions->getNumberOfIgnoredLines(),
            usingTypes: $usingTypes,
            session: $importOptions->getSession(),
            importAsNull: $importOptions->importAsNull(),
            features: $importOptions->features(),
        );
    }

    private function importByTableCopy(
        BigQueryClient $bqClient,
        CommandDestination $destination,
        ImportOptions $importOptions,
        Table $source,
        ?SqlSourceInterface $filteredSource,
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
                $destination,
                $importOptions,
                $source,
                $bigqueryImportOptions,
                $sourceMapping,
                $filteredSource,
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
