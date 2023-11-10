<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryInputDataException;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Storage\GCS\SourceFile;
use Keboola\FileStorage\Gcs\GcsProvider;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\CreateImportOptionHelper;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\Import\ImportValidationException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Throwable;

final class ImportTableFromFileHandler extends BaseHandler
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
     * @param TableImportFromFileCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof TableImportFromFileCommand);

        assert($runtimeOptions->getMeta() === null);

        // validate
        assert(
            $command->getFileProvider() === FileProvider::GCS,
            'Only S3 is supported TableImportFromFileCommand.fileProvider.'
        );
        assert(
            $command->getFileFormat() === FileFormat::CSV,
            'Only CSV is supported TableImportFromFileCommand.fileFormat.'
        );
        $any = $command->getFormatTypeOptions();
        assert($any !== null, 'TableImportFromFileCommand.formatTypeOptions is required.');
        $formatOptions = $any->unpack();
        assert($formatOptions instanceof TableImportFromFileCommand\CsvTypeOptions);
        assert(
            $formatOptions->getSourceType() !== TableImportFromFileCommand\CsvTypeOptions\SourceType::DIRECTORY,
            'TableImportFromFileCommand.formatTypeOptions.sourceType directory is not supported.'
        );
        assert($command->hasFilePath() === true, 'TableImportFromFileCommand.filePath is required.');
        $destination = $command->getDestination();
        assert($destination !== null, 'TableImportFromFileCommand.destination is required.');
        $importOptions = $command->getImportOptions();
        assert($importOptions !== null, 'TableImportFromFileCommand.importOptions is required.');

        $csvOptions = new CsvOptions(
            $formatOptions->getDelimiter(),
            $formatOptions->getEnclosure(),
            $formatOptions->getEscapedBy()
        );

        $filePath = $command->getFilePath();
        assert($filePath !== null);
        $source = $this->getSourceFile($filePath, $credentials, $csvOptions, $formatOptions);
        $bigqueryImportOptions = CreateImportOptionHelper::createOptions($importOptions);

        $stagingTable = null;
        $bqClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);
        $destinationRef = new BigqueryTableReflection(
            $bqClient,
            ProtobufHelper::repeatedStringToArray($destination->getPath())[0],
            $destination->getTableName()
        );
        try {
            /** @var BigqueryTableDefinition $destinationDefinition */
            $destinationDefinition = $destinationRef->getTableDefinition();
            $dedupColumns = ProtobufHelper::repeatedStringToArray($importOptions->getDedupColumnsNames());
            if ($importOptions->getDedupType() === ImportOptions\DedupType::UPDATE_DUPLICATES
                && count($dedupColumns) !== 0
            ) {
                $destinationDefinition = new BigqueryTableDefinition(
                    $destinationDefinition->getSchemaName(),
                    $destination->getTableName(),
                    $destinationDefinition->isTemporary(),
                    $destinationDefinition->getColumnsDefinitions(),
                    $dedupColumns, // add dedup columns separately as BQ has no primary keys
                );
            }
            // prepare staging table definition
            $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
                $destinationDefinition,
                $source->getColumnsNames()
            );
            // create staging table
            $qb = new BigqueryTableQueryBuilder();
            $query = $bqClient->query(
                $qb->getCreateTableCommandFromDefinition($stagingTable)
            );
            $bqClient->runQuery($query);

            // load to staging table
            $toStageImporter = new ToStageImporter($bqClient);
            $importState = $toStageImporter->importToStagingTable(
                $source,
                $stagingTable,
                $bigqueryImportOptions
            );
            // import data to destination
            $toFinalTableImporter = new FullImporter($bqClient);
            if ($bigqueryImportOptions->isIncremental()) {
                $toFinalTableImporter = new IncrementalImporter($bqClient);
            }
            $importResult = $toFinalTableImporter->importToTable(
                $stagingTable,
                $destinationDefinition,
                $bigqueryImportOptions,
                $importState
            );
        } catch (BigqueryInputDataException $e) {
            throw new ImportValidationException($e->getMessage());
        } finally {
            if ($stagingTable !== null) {
                try {
                    $query = (new BigqueryTableQueryBuilder())->getDropTableCommand(
                        $stagingTable->getSchemaName(),
                        $stagingTable->getTableName()
                    );
                    $bqClient->runQuery($bqClient->query($query));
                } catch (Throwable $e) {
                    // ignore
                }
            }
        }

        $response = new TableImportResponse();
        $destinationRef->refresh();
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

    private function getSourceFile(
        FilePath $filePath,
        GenericBackendCredentials $credentials,
        CsvOptions $csvOptions,
        TableImportFromFileCommand\CsvTypeOptions $formatOptions
    ): SourceFile {
        $relativePath = RelativePath::create(
            new GcsProvider(),
            $filePath->getRoot(),
            $filePath->getPath(),
            $filePath->getFileName()
        );

        return new SourceFile(
            $relativePath->getRoot(),
            $relativePath->getPathnameWithoutRoot(),
            'name',
            CredentialsHelper::getCredentialsArray($credentials),
            $csvOptions,
            $formatOptions->getSourceType() === TableImportFromFileCommand\CsvTypeOptions\SourceType::SLICED_FILE,
            ProtobufHelper::repeatedStringToArray($formatOptions->getColumnsNames()),
            [] // <-- ignore primary keys here should be deprecated
        );
    }
}
