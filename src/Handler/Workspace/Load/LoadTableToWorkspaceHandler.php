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
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\Exception\ColumnsMismatchException;
use Keboola\Db\ImportExport\Storage\Bigquery\SelectSource;
use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\CreateImportOptionHelper;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\BigQuery\Handler\Table\ObjectAlreadyExistsException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\BadExportFilterParametersException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\ColumnsMismatchException as DriverColumnsMismatchException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\MaximumLengthOverflowException;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
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
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use LogicException;
use Throwable;

class LoadTableToWorkspaceHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    private ?LoadSourceFactory $sourceFactory = null;

    private ?ColumnMappingService $columnMappingService = null;

    private ?LoadDestinationManager $destinationManager = null;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
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
        if ($importOptions->getImportType() === ImportType::INCREMENTAL) {
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
        if ($options->getImportType() === ImportType::FULL) {
            $loadFromStringTable = $options->getImportStrategy() === ImportStrategy::STRING_TABLE;

            if (!$loadFromStringTable) {
                try {
                    Assert::assertSameColumnsOrdered(
                        $sourceTableDefinition->getColumnsDefinitions(),
                        $destinationDefinition->getColumnsDefinitions(),
                        [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
                        [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
                    );
                } catch (ColumnsMismatchException $e) {
                    // convert the exception from IE ex to driver ex
                    throw new DriverColumnsMismatchException($e->getMessage());
                }
            }
            [$columnNameMappingRequired, $dataCastingRequired]
                = $this->checkMappingDifferences($sourceTableDefinition, $destinationDefinition, $sourceMapping);
            $dedupRequired = $options->getDedupType() === ImportOptions\DedupType::UPDATE_DUPLICATES
                && $destinationDefinition->getPrimaryKeysNames() !== [];

            $filterRequired = $source instanceof SelectSource;
            switch (true) {
                    // case 24
                case $loadFromStringTable && !$dataCastingRequired && !$columnNameMappingRequired && !$dedupRequired:
                    // case 31.1,32.1
                case !$loadFromStringTable && !$dataCastingRequired && !$columnNameMappingRequired && $filterRequired:
                    // case 22
                case $loadFromStringTable && $columnNameMappingRequired && !$dataCastingRequired && !$dedupRequired:
                    $toStageImporter = new ToStageImporter($bqClient);
                    try {
                        $importState = $toStageImporter->importToStagingTable(
                            $source,
                            $destinationDefinition,
                            $importOptions,
                        );
                    } catch (ColumnsMismatchException $e) {
                        throw new ColumnsMismatchException($e->getMessage());
                    }
                    $importState->setImportedColumns($destinationDefinition->getColumnsNames());

                    return [null, $importState->getResult()];

                    // case 17 and 18 src is string and it will casted and mapped
                case $loadFromStringTable && $dataCastingRequired && $columnNameMappingRequired:
                    // case 21
                case $loadFromStringTable && $columnNameMappingRequired && !$dataCastingRequired && $dedupRequired:
                    // case 23
                case $loadFromStringTable && !$dataCastingRequired && !$columnNameMappingRequired && $dedupRequired:
                // case 19.1,20.1
                case $loadFromStringTable && $dataCastingRequired && !$columnNameMappingRequired && $filterRequired:
                    // staging table + full importer

                    // prepare the staging table definition here to identify if the columns are identical or not
                    /** @var ColumnMapping[] $mappings */
                    $mappings = iterator_to_array($sourceMapping->getColumnMappings()->getIterator());
                    // load to staging table
                    $stagingTable = StageTableDefinitionFactory::createStagingTableDefinitionWithMapping(
                        $sourceTableDefinition,
                        $mappings,
                    );

                    // TODO try CopyImportFromTableToTable

                    $bqClient->runQuery($bqClient->query(
                        (new BigqueryTableQueryBuilder())->getCreateTableCommand(
                            $stagingTable->getSchemaName(),
                            $stagingTable->getTableName(),
                            $stagingTable->getColumnsDefinitions(),
                            [], // TODO
                        ),
                    ));
                    // Use standard SQL-based import with INSERT INTO ... SELECT
                    // This provides more control for column transformations, filters, and deduplication
                    $toStageImporter = new ToStageImporter($bqClient);
                    $importState = $toStageImporter->importToStagingTable(
                        $source,
                        $stagingTable,
                        $importOptions,
                    );

                    $importer = new FullImporter($bqClient);
                    try {
                        $importResult = $importer->importToTable(
                            $stagingTable,
                            $destinationDefinition,
                            $importOptions,
                            $importState,
                        );
                    } catch (ColumnsMismatchException $e) {
                        throw new DriverColumnsMismatchException($e->getMessage());
                    } catch (BigqueryException $e) {
                        throw new BigqueryInputDataException($e->getMessage());
                    }
                    return [$stagingTable, $importResult];
                    // case 19.2,20.2
                case $loadFromStringTable && $dataCastingRequired && !$columnNameMappingRequired && !$filterRequired:
                    // case 31.2,32.2
                case !$loadFromStringTable && $dataCastingRequired && !$columnNameMappingRequired && !$filterRequired:
                    /* full importer - case 19, 20; 31,32
                     * one of following options
                     * 1. src table is typed but no changes on casting/mapping (31.2,32.2)
                     * 2. src table is string and casting required (19,20)
                     */

                    $importer = new FullImporter($bqClient);
                    try {
                        $importResult = $importer->importToTable(
                            $sourceTableDefinition,
                            $destinationDefinition,
                            $importOptions,
                            new ImportState($destinationDefinition->getTableName()),
                        );
                    } catch (ColumnsMismatchException $e) {
                        throw new DriverColumnsMismatchException($e->getMessage());
                    } catch (BigqueryException $e) {
                        throw new BigqueryInputDataException($e->getMessage());
                    }
                    return [null, $importResult];
                default:
                    // should not happen, just to cover the case
                    throw new LogicException(
                        sprintf(
                            'Invalid import options: '
                            . '$loadFromStringTable: %s, '
                            . '$dataCastingRequired: %s, '
                            . '$columnNameMappingRequired: %s',
                            $loadFromStringTable,
                            $dataCastingRequired,
                            $columnNameMappingRequired,
                        ),
                    );
            }
        } else {
            // prepare the staging table definition here to identify if the columns are identical or not
            /** @var ColumnMapping[] $mappings */
            $mappings = iterator_to_array($sourceMapping->getColumnMappings()->getIterator());
            // load to staging table
            $stagingTable = StageTableDefinitionFactory::createStagingTableDefinitionWithMapping(
                $sourceTableDefinition,
                $mappings,
            );

            // TODO try CopyImportFromTableToTable
            $dedupColumns = ProtobufHelper::repeatedStringToArray($options->getDedupColumnsNames());
            $bqClient->runQuery($bqClient->query(
                (new BigqueryTableQueryBuilder())->getCreateTableCommand(
                    $stagingTable->getSchemaName(),
                    $stagingTable->getTableName(),
                    $stagingTable->getColumnsDefinitions(),
                    $dedupColumns, // not really needed, but keep
                ),
            ));
            // Use standard SQL-based import with INSERT INTO ... SELECT
            // This provides more control for column transformations, filters, and deduplication
            $toStageImporter = new ToStageImporter($bqClient);

            try {
                $importState = $toStageImporter->importToStagingTable(
                    $source,
                    $stagingTable,
                    $importOptions,
                );

                $toFinalTableImporter = new IncrementalImporter($bqClient);
                $importResult = $toFinalTableImporter->importToTable(
                    $stagingTable,
                    $destinationDefinition,
                    $importOptions,
                    $importState,
                );
            } catch (ColumnsMismatchException $e) {
                throw new DriverColumnsMismatchException($e->getMessage());
            } catch (BigqueryException $e) {
                throw new BigqueryInputDataException($e->getMessage());
            }
        }
        return [$stagingTable, $importResult];
    }

    private function importByTableCopy(
        LoadContext $context,
    ): Result {
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
        } catch (BigqueryException $e) {
            $handled = MaximumLengthOverflowException::handleException($e);
            if ($handled instanceof MaximumLengthOverflowException) {
                throw $handled;
            }

            $handled = BadExportFilterParametersException::handleWrongTypeInFilters($e);
            if ($handled instanceof BadExportFilterParametersException) {
                throw $handled;
            }

            throw new ImportValidationException(DecodeErrorMessage::getErrorMessage($e));
        } catch (DriverColumnsMismatchException $e) {
            throw new ImportValidationException($e->getMessage());
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
    private function clone(
        BigQueryClient $bqClient,
        CommandDestination $destination,
        Table $source,
    ): Result {
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

    /**
     * @return bool[]
     */
    protected function checkMappingDifferences(
        BigqueryTableDefinition $sourceTableDefinition,
        BigqueryTableDefinition $destTableDefinition,
        SourceTableMapping $sourceMapping,
    ): array {
        $columnNameMappingRequired = false;
        $dataCastingRequired = false;
        $srcDefinitions = iterator_to_array($sourceTableDefinition->getColumnsDefinitions());
        $destDefinitions = iterator_to_array($destTableDefinition->getColumnsDefinitions());
        $fn = function (string $columnName, array $definitions) {
            foreach ($definitions as $definition) {
                if ($columnName === $definition->getColumnName()) {
                    return $definition;
                }
            }
            return null;
        };
        foreach ($sourceMapping->getColumnMappings() as $mapping) {
            /** @var ColumnMapping $mapping */
            $srcColumnName = $mapping->getSourceColumnName();
            $destColumnName = $mapping->getDestinationColumnName();
            if ($srcColumnName !== $destColumnName) {
                $columnNameMappingRequired = true;
            }
            /** @var BigqueryColumn $srcDef */
            $srcDef = $fn($srcColumnName, $srcDefinitions);
            $destDef = $fn($destColumnName, $destDefinitions);
            assert($srcDef !== null);
            assert($destDef !== null);

            // casting needed for types difference only. If the difference is on nullability, length, default, it is ok
            if ($srcDef->getColumnDefinition()->getType() !== $destDef->getColumnDefinition()->getType()) {
                $dataCastingRequired = true;
            }
        }
        return [$columnNameMappingRequired, $dataCastingRequired];
    }
}
