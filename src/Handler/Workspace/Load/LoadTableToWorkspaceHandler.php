<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Load;

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
use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\CreateImportOptionHelper;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\BigQuery\Handler\Table\ObjectAlreadyExistsException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\BadExportFilterParametersException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\ColumnsMismatchException as DriverColumnsMismatchException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceLib\CopyImportFromTableToTable;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\MaximumLengthOverflowException;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table as CommandDestination;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand\SourceTableMapping;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\Import\ImportValidationException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use LogicException;
use Throwable;

class LoadTableToWorkspaceHandler extends BaseHandler
{
    public GCPClientManager $clientManager;
    private ?LoadSourceFactory $sourceFactory;
    private ?ColumnMappingService $columnMappingService;
    private ?LoadDestinationManager $destinationManager;

    public function __construct(
        GCPClientManager $clientManager,
        ?LoadSourceFactory $sourceFactory = null,
        ?ColumnMappingService $columnMappingService = null,
        ?LoadDestinationManager $destinationManager = null,
    ) {
        parent::__construct();
        $this->clientManager = $clientManager;
        $this->sourceFactory = $sourceFactory;
        $this->columnMappingService = $columnMappingService;
        $this->destinationManager = $destinationManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param LoadTableToWorkspaceCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof LoadTableToWorkspaceCommand);

        // Validate required command fields
        $sourceMapping = $command->getSource();
        assert($sourceMapping !== null, 'LoadTableToWorkspaceCommand.source is required.');
        $destination = $command->getDestination();
        assert($destination !== null, 'LoadTableToWorkspaceCommand.destination is required.');
        $importOptions = $command->getImportOptions();
        assert($importOptions !== null, 'LoadTableToWorkspaceCommand.importOptions is required.');

        /** @var array<string, string> $queryTags */
        $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

        $bqClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            $queryTags,
        );

        // Instantiate services if not injected (for backward compatibility)
        $sourceFactory = $this->sourceFactory ?? new LoadSourceFactory($bqClient);
        $columnMapping = $this->columnMappingService ?? new ColumnMappingService();
        $destinationManager = $this->destinationManager ?? new LoadDestinationManager($bqClient);

        // Create source using factory
        $sourceContext = $sourceFactory->createFromCommand($command);
        $bigqueryImportOptions = CreateImportOptionHelper::createOptions($importOptions, $features);

        // Build destination columns from source and mapping
        $expectedDestinationColumns = $columnMapping->buildDestinationColumns(
            $sourceContext->effectiveDefinition,
            $sourceMapping,
        );

        // Resolve or create destination table
        $destinationDefinition = $destinationManager->resolveDestination(
            $destination,
            $importOptions,
            $expectedDestinationColumns,
        );

        // Validate incremental destination if needed
        if ($importOptions->getImportType() === ImportType::INCREMENTAL
            && $importOptions->getDedupType() === ImportOptions\DedupType::UPDATE_DUPLICATES
        ) {
            $destinationManager->validateIncrementalDestination(
                $destinationDefinition,
                $expectedDestinationColumns,
                $sourceContext->fullDefinition,
            );
        }

        // Handle REPLACE mode for VIEW/PBCLONE imports
        $shouldDropTableIfExists = $importOptions->getCreateMode() === ImportOptions\CreateMode::REPLACE
            && in_array($importOptions->getImportType(), [ImportType::VIEW, ImportType::PBCLONE], true);

        if ($shouldDropTableIfExists) {
            $dataset = $bqClient->dataset(ProtobufHelper::repeatedStringToArray($destination->getPath())[0]);
            $table = $dataset->table($destination->getTableName());
            if ($table->exists()) {
                $table->delete();
            }
        }

        // Create load context and execute
        $context = LoadContext::create(
            $bqClient,
            $destination,
            $destinationDefinition,
            $importOptions,
            $sourceContext->source,
            $sourceContext->effectiveDefinition,
            $bigqueryImportOptions,
            $sourceMapping,
        );

        $importResult = $this->executeImport($context);

        // Build and return response
        return $this->buildResponse($bqClient, $destination, $importResult);
    }

    /**
     * Execute load operation based on import type
     */
    private function executeImport(LoadContext $context): Result
    {
        switch ($context->importOptions->getImportType()) {
            case ImportType::FULL:
            case ImportType::INCREMENTAL:
                return $this->importByTableCopy($context);
            case ImportType::VIEW:
                assert($context->source instanceof Table);
                return $this->createView($context->bqClient, $context->destination, $context->source);
            case ImportType::PBCLONE:
                assert($context->source instanceof Table);
                return $this->clone($context->bqClient, $context->destination, $context->source);
            default:
                throw new LogicException(sprintf(
                    'Unknown import type "%s".',
                    $context->importOptions->getImportType(),
                ));
        }
    }

    /**
     * Build response with load results
     */
    private function buildResponse(
        BigQueryClient $bqClient,
        CommandDestination $destination,
        Result $importResult,
    ): TableImportResponse {
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
     * @return array{0: BigqueryTableDefinition|null, 1: Result}
     */
    private function import(
        BigQueryClient $bqClient,
        BigqueryTableDefinition $destinationDefinition,
        ImportOptions $options,
        SqlSourceInterface $source,
        BigqueryTableDefinition $sourceTableDefinition,
        BigqueryImportOptions $importOptions,
        SourceTableMapping $sourceMapping,
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

        /** @var ColumnMapping[] $mappings */
        $mappings = iterator_to_array($sourceMapping->getColumnMappings()->getIterator());
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

        // Determine if we need deduplication
        // COPY optimization copies all rows including duplicates, which is problematic when:
        // - Import type is INCREMENTAL (merging with existing data)
        // - Dedup type is UPDATE_DUPLICATES (need to merge duplicate PK values)
        // - Dedup columns are specified (PK columns exist)
        // In this case, the source table may contain duplicates that need deterministic resolution.
        $needsDeduplication = (
            $options->getImportType() === ImportType::INCREMENTAL &&
            $options->getDedupType() === ImportOptions\DedupType::UPDATE_DUPLICATES &&
            count($dedupColumns) > 0
        );

        if ($isColumnIdentical && $source instanceof Table && !$needsDeduplication) {
            // OPTIMIZATION: use BigQuery native COPY to transfer table to staging
            // COPY copies all rows including duplicates, which is safe when:
            // - FULL imports (destination is empty, dedup happens later if needed)
            // - INSERT_DUPLICATES mode (duplicates already handled in source)
            // - Incremental without dedup (duplicates are allowed)
            // NOT safe when: Incremental + UPDATE_DUPLICATES (source may have dups, need deterministic dedup)
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
            // Use standard SQL-based import with INSERT INTO ... SELECT
            // This provides more control for column transformations, filters, and deduplication
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

    private function importByTableCopy(LoadContext $context): Result
    {
        $stagingTable = null;
        try {
            [
                $stagingTable,
                $importResult,
            ] = $this->import(
                $context->bqClient,
                $context->destinationDefinition,
                $context->importOptions,
                $context->source,
                $context->sourceTableDefinition,
                $context->bigqueryImportOptions,
                $context->sourceMapping,
            );
        } catch (BigqueryInputDataException $e) {
            throw new ImportValidationException(DecodeErrorMessage::getErrorMessage($e));
        } catch (BigqueryException $e) {
            throw MaximumLengthOverflowException::handleException($e);
        } finally {
            if ($stagingTable !== null) {
                try {
                    $context->bqClient->runQuery($context->bqClient->query(
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
