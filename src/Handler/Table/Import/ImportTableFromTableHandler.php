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
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\CreateImportOptionHelper;
use Keboola\StorageDriver\BigQuery\Handler\Table\BadExportFilterParametersException;
use Keboola\StorageDriver\BigQuery\Handler\Table\ObjectAlreadyExistsException;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table as CommandDestination;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\Import\ImportValidationException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use LogicException;
use Throwable;

class ImportTableFromTableHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
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
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof TableImportFromTableCommand);

        // validate
        $sourceMapping = $command->getSource();
        assert($sourceMapping !== null, 'TableImportFromFileCommand.source is required.');
        $destination = $command->getDestination();
        assert($destination !== null, 'TableImportFromFileCommand.destination is required.');
        $importOptions = $command->getImportOptions();
        assert($importOptions !== null, 'TableImportFromFileCommand.importOptions is required.');

        $bqClient = $this->clientManager->getBigQueryClient($credentials);

        $source = $this->createSource($bqClient, $command);
        $bigqueryImportOptions = CreateImportOptionHelper::createOptions($importOptions);

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
                    $bigqueryImportOptions,
                    $sourceMapping
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
                    $importOptions->getImportType()
                ));
        }

        $response = new TableImportResponse();
        $destinationRef = new BigqueryTableReflection(
            $bqClient,
            ProtobufHelper::repeatedStringToArray($destination->getPath())[0],
            $destination->getTableName()
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

    private function createSource(
        BigQueryClient $bqClient,
        TableImportFromTableCommand $command
    ): Table {
        $sourceMapping = $command->getSource();
        assert($sourceMapping !== null);
        $sourceColumns = [];
        /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping $mapping */
        foreach ($sourceMapping->getColumnMappings() as $mapping) {
            $sourceColumns[] = $mapping->getSourceColumnName();
        }
        $sourceTableDef = (new BigqueryTableReflection(
            $bqClient,
            ProtobufHelper::repeatedStringToArray($sourceMapping->getPath())[0],
            $sourceMapping->getTableName()
        ))->getTableDefinition();
        return new Table(
            ProtobufHelper::repeatedStringToArray($sourceMapping->getPath())[0],
            $sourceMapping->getTableName(),
            $sourceColumns,
            $sourceTableDef->getPrimaryKeysNames()
        );
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
        TableImportFromTableCommand\SourceTableMapping $sourceMapping
    ): array {
        /** @var BigqueryTableDefinition $destinationDefinition */
        $destinationDefinition = (new BigqueryTableReflection(
            $bqClient,
            ProtobufHelper::repeatedStringToArray($destination->getPath())[0],
            $destination->getTableName()
        ))->getTableDefinition();
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
            $importState = $toStageImporter->importToStagingTable(
                $source,
                $destinationDefinition,
                $importOptions
            );
            return [null, $importState->getResult()];
        }

        $stagingTable = $this->createStageTable($destinationDefinition, $sourceMapping, $bqClient);
        // load to staging table
        $toStageImporter = new ToStageImporter($bqClient);
        $importState = $toStageImporter->importToStagingTable(
            $source,
            $stagingTable,
            $importOptions
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
            $importState
        );
        return [$stagingTable, $importResult];
    }

    private function createStageTable(
        BigqueryTableDefinition $destinationDefinition,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
        BigQueryClient $bqClient
    ): BigqueryTableDefinition {
        // prepare staging table definition
        /** @var TableImportFromTableCommand\SourceTableMapping\ColumnMapping[] $mappings */
        $mappings = iterator_to_array($sourceMapping->getColumnMappings()->getIterator());
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinitionWithMapping(
            $destinationDefinition,
            $mappings
        );
        // create staging table
        $qb = new BigqueryTableQueryBuilder();
        $bqClient->runQuery($bqClient->query(
            $qb->getCreateTableCommand(
                $stagingTable->getSchemaName(),
                $stagingTable->getTableName(),
                $stagingTable->getColumnsDefinitions(),
                [] //<-- there are no PK in BQ
            )
        ));
        return $stagingTable;
    }

    private function importByTableCopy(
        BigQueryClient $bqClient,
        CommandDestination $destination,
        ImportOptions $importOptions,
        Table $source,
        BigqueryImportOptions $bigqueryImportOptions,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping
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
                $sourceMapping
            );
        } finally {
            if ($stagingTable !== null) {
                try {
                    $bqClient->runQuery($bqClient->query(
                        (new BigqueryTableQueryBuilder())->getDropTableCommand(
                            $stagingTable->getSchemaName(),
                            $stagingTable->getTableName()
                        )
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
        Table $source
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
                $bqClient->query($sql)
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
                $bqClient->query($sql)
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
            $bqClient->query($sql)
        );

        return new Result([
            'importedRowsCount' => (new BigqueryTableReflection(
                $bqClient,
                ProtobufHelper::repeatedStringToArray($destination->getPath())[0],
                $destination->getTableName()
            ))->getRowsCount(),
        ]);
    }
}
