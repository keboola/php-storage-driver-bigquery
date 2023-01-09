<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Alter;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ExportQueryBuilder;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsCommand;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportFilters;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

final class DeleteTableRowsHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param DeleteTableRowsCommand $command
     * @return DeleteTableRowsResponse
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DeleteTableRowsCommand);

        assert($command->getPath()->count() === 1, 'AddColumnCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'AddColumnCommand.tableName is required');

        $bqClient = $this->clientManager->getBigQueryClient($credentials);
        /** @var string $datasetName */
        $datasetName = $command->getPath()[0];

        $this->validateFilters($command);

        // build sql
        $queryBuilder = new ExportQueryBuilder($bqClient, new ColumnConverter());
        $ref = new BigqueryTableReflection($bqClient, $datasetName, $command->getTableName());
        $tableColumnsDefinitions = $ref->getColumnsDefinitions();

        $queryData = $queryBuilder->buildQueryFromCommand(
            ExportQueryBuilder::MODE_DELETE,
            (new ExportFilters())
                ->setChangeSince($command->getChangeSince())
                ->setChangeUntil($command->getChangeUntil())
                ->setWhereFilters($command->getWhereFilters()),
            new RepeatedField(GPBType::MESSAGE, ExportOrderBy::class),
            new RepeatedField(GPBType::STRING),
            $tableColumnsDefinitions,
            $datasetName,
            $command->getTableName(),
            false
        );
        /** @var array<string> $queryDataBindings */
        $queryDataBindings = $queryData->getBindings();

        $initialRowsCount = $ref->getRowsCount();

        $res = $bqClient->runQuery(
            $bqClient->query($queryData->getQuery())
                ->parameters($queryDataBindings)
        );

        $stats = $ref->getTableStats();

        return (new DeleteTableRowsResponse())
            ->setDeletedRowsCount($initialRowsCount - $stats->getRowsCount())
            ->setTableRowsCount($stats->getRowsCount())
            ->setTableSizeBytes($stats->getDataSizeBytes())
            ;
    }

    private function validateFilters(DeleteTableRowsCommand $command): void
    {
        if ($command->getChangeSince() !== '') {
            assert(
                is_numeric($command->getChangeSince()),
                'PreviewTableCommand.changeSince must be numeric timestamp'
            );
        }
        if ($command->getChangeUntil() !== '') {
            assert(
                is_numeric($command->getChangeUntil()),
                'PreviewTableCommand.changeUntil must be numeric timestamp'
            );
        }
    }
}
