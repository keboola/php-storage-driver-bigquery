<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Export;

use Google\Cloud\Core\Exception\BadRequestException;
use Google\Protobuf\Internal\Message;
use Keboola\Db\ImportExport\Backend\Bigquery\Export\Exporter;
use Keboola\Db\ImportExport\ExportOptions as ExportOptionsLib;
use Keboola\Db\ImportExport\Storage\Bigquery\SelectSource;
use Keboola\Db\ImportExport\Storage\GCS\DestinationFile;
use Keboola\FileStorage\Gcs\GcsProvider;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Table\BadExportFilterParameters;
use Keboola\StorageDriver\BigQuery\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ExportQueryBuilder;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

class ExportTableToFileHandler implements DriverCommandHandlerInterface
{
    private GCPClientManager $clientManager;

    public function __construct(
        GCPClientManager $clientManager
    ) {
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param TableExportToFileCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof TableExportToFileCommand);

        // validate
        $source = $command->getSource();
        assert($source !== null, 'TableExportToFileCommand.source is required.');
        assert(
            $source->getPath()->count() === 1,
            'TableExportToFileCommand.source.path is required and size must equal 1'
        );
        assert(!empty($source->getTableName()), 'TableExportToFileCommand.source.tableName is required');

        assert(
            $command->getFileProvider() === FileProvider::GCS,
            'Only GCS is supported TableExportToFileCommand.fileProvider.'
        );

        assert(
            $command->getFileFormat() === FileFormat::CSV,
            'Only CSV is supported TableExportToFileCommand.fileFormat.'
        );
        assert($command->getFilePath() !== null, 'TableExportToFileCommand.filePath is required.');

        $requestExportOptions = $command->getExportOptions() ?? new ExportOptions();
        $columnsToExport = ProtobufHelper::repeatedStringToArray($requestExportOptions->getColumnsToExport());
        $exportOptions = $this->createOptions(
            $requestExportOptions
        );

        $bqClient = $this->clientManager->getBigQueryClient($credentials);
        $queryBuilder = new ExportQueryBuilder($bqClient, new ColumnConverter());
        $datasetName = ProtobufHelper::repeatedStringToArray($source->getPath())[0];
        $tableColumnsDefinitions = (new BigqueryTableReflection($bqClient, $datasetName, $source->getTableName()))
            ->getColumnsDefinitions();
        $queryData = $queryBuilder->buildQueryFromCommand(
            ExportQueryBuilder::MODE_SELECT,
            $requestExportOptions->getFilters(),
            $requestExportOptions->getOrderBy(),
            $requestExportOptions->getColumnsToExport(),
            $tableColumnsDefinitions,
            $datasetName,
            $source->getTableName(),
            false
        );
        /** @var array<string> $queryDataBindings */
        $queryDataBindings = $queryData->getBindings();

        $sourceRef = new SelectSource(
            $queryData->getQuery(),
            $queryDataBindings,
            $queryData->getTypes(),
            $columnsToExport
        );

        $destinationRef = $this->getDestinationFile(
            $command->getFilePath(),
            $credentials
        );

        try {
            (new Exporter($bqClient))->exportTable(
                $sourceRef,
                $destinationRef,
                $exportOptions
            );
        } catch (BadRequestException $e) {
            BadExportFilterParameters::handleWrongTypeInFilters($e);
            throw $e;
        }

        return (new TableExportToFileResponse())
            ->setTableInfo(
                TableReflectionResponseTransformer::transformTableReflectionToResponse(
                    $datasetName,
                    new BigqueryTableReflection(
                        $bqClient,
                        $datasetName,
                        $source->getTableName()
                    )
                )
            );
    }

    private function getDestinationFile(
        FilePath $filePath,
        GenericBackendCredentials $credentials
    ): DestinationFile {
        $relativePath = RelativePath::create(
            new GcsProvider(),
            $filePath->getRoot(),
            $filePath->getPath(),
            $filePath->getFileName(),
        );
        return new DestinationFile(
            $relativePath->getRoot(),
            $relativePath->getPathnameWithoutRoot(),
            '',
            CredentialsHelper::getCredentialsArray($credentials)
        );
    }

    private function createOptions(
        ?ExportOptions $options
    ): ExportOptionsLib {
        return new ExportOptionsLib(
            $options && $options->getIsCompressed(),
            ExportOptionsLib::MANIFEST_AUTOGENERATED
        );
    }
}
