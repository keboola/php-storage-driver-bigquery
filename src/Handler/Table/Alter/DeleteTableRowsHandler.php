<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Alter;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ExportQueryBuilder;
use Keboola\StorageDriver\BigQuery\QueryBuilder\QueryBuilderResponse;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsCommand;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportFilters;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

final class DeleteTableRowsHandler extends BaseHandler
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
     * @param DeleteTableRowsCommand $command
     * @return DeleteTableRowsResponse
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DeleteTableRowsCommand);

        assert($runtimeOptions->getMeta() === null);

        assert($command->getPath()->count() === 1, 'AddColumnCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'AddColumnCommand.tableName is required');

        /** @var array<string, string> $queryTags */
        $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

        $bqClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            $queryTags,
        );
        /** @var string $datasetName */
        $datasetName = $command->getPath()[0];

        $this->validateFilters($command);

        // build sql
        $queryBuilder = new ExportQueryBuilder($bqClient, new ColumnConverter());
        $ref = new BigqueryTableReflection($bqClient, $datasetName, $command->getTableName());
        $tableColumnsDefinitions = $ref->getColumnsDefinitions();

        if ($this->areFiltersEmpty($command)) {
            // truncate table
            $queryData = new QueryBuilderResponse(
                sprintf(
                    'TRUNCATE TABLE %s.%s',
                    BigqueryQuote::quoteSingleIdentifier($datasetName),
                    BigqueryQuote::quoteSingleIdentifier($command->getTableName()),
                ),
                [],
            );
        } else {
            // delete from
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
                false,
                refTableFilters: $command->getWhereRefTableFilters(),
            );
        }
        /** @var array<string> $queryDataBindings */
        $queryDataBindings = $queryData->getBindings();
        $initialRowsCount = $ref->getRowsCount();

        $bqClient->runQuery(
            $bqClient->query($queryData->getQuery())
                ->parameters($queryDataBindings),
        );

        $ref->refresh();
        $stats = $ref->getTableStats();

        return (new DeleteTableRowsResponse())
            ->setDeletedRowsCount($initialRowsCount - $stats->getRowsCount())
            ->setTableRowsCount($stats->getRowsCount())
            ->setTableSizeBytes($stats->getDataSizeBytes());
    }

    private function validateFilters(DeleteTableRowsCommand $command): void
    {
        if ($command->getChangeSince() !== '') {
            assert(
                is_numeric($command->getChangeSince()),
                'PreviewTableCommand.changeSince must be numeric timestamp',
            );
        }
        if ($command->getChangeUntil() !== '') {
            assert(
                is_numeric($command->getChangeUntil()),
                'PreviewTableCommand.changeUntil must be numeric timestamp',
            );
        }
    }

    private function areFiltersEmpty(DeleteTableRowsCommand $command): bool
    {
        return $command->getChangeSince() === ''
            && $command->getChangeUntil() === ''
            && count($command->getWhereFilters()) === 0
            && count($command->getWhereRefTableFilters()) === 0;
    }
}
