<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Profile;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\Column\DistinctCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\NullCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\ColumnCountTableMetric;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\BigQuery\Profile\DataSizeTableMetric;
use Keboola\StorageDriver\BigQuery\Profile\RowCountTableMetric;
use Keboola\StorageDriver\BigQuery\Profile\TableMetricInterface;
use Keboola\StorageDriver\Command\Table\CreateProfileTableCommand;
use Keboola\StorageDriver\Command\Table\CreateProfileTableResponse;
use Keboola\StorageDriver\Command\Table\CreateProfileTableResponse\Column;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class ProfileTableHandler extends BaseHandler
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
     * @param CreateProfileTableCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message|null {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateProfileTableCommand);

        assert($runtimeOptions->getMeta() === null);

        // Validate
        assert($command->getPath()->count() === 1, 'DropTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'DropTableCommand.tableName is required');

        $bigQuery = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            iterator_to_array($runtimeOptions->getQueryTags()),
        );
        /** @var string $datasetName */
        $datasetName = $command->getPath()[0]; // bucket name
        $dateset = $bigQuery->dataset($datasetName);
        $table = $dateset->table($command->getTableName());
        $bigQueryContext = new BigQueryContext(
            $bigQuery,
            $table,
        );

        $response = (new CreateProfileTableResponse())
            ->setPath($command->getPath())
            ->setTableName($command->getTableName());

        /** @var TableMetricInterface[] $tableMetrics */
        $tableMetrics = [
            new RowCountTableMetric(),
            new ColumnCountTableMetric(),
            new DataSizeTableMetric(),
        ];
        $tableProfile = [];

        foreach ($tableMetrics as $metric) {
            $tableProfile[$metric->name()] = $metric->collect(
                $datasetName,
                $command->getTableName(),
                $bigQueryContext,
            );
        }

        $response->setProfile(json_encode($tableProfile, JSON_THROW_ON_ERROR));

        /** @var ColumnMetricInterface[] $columnMetrics */
        $columnMetrics = [
            new DistinctCountColumnMetric(),
            new DuplicateCountColumnMetric(),
            new NullCountColumnMetric(),
        ];
        $columnProfiles = [];

        $columns = $table->info()['schema']['fields'];
        foreach ($columns as $column) {
            $columnProfile = [];

            foreach ($columnMetrics as $metric) {
                $columnProfile[$metric->name()] = $metric->collect(
                    $datasetName,
                    $command->getTableName(),
                    $column['name'],
                    $bigQueryContext,
                );
            }

            $columnProfiles[] = (new Column())
                ->setName($column['name'])
                ->setProfile(json_encode($columnProfile, JSON_THROW_ON_ERROR));
        }

        $response->setColumns($columnProfiles);

        return $response;
    }
}
