<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Profile;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Profile\Column\DistinctCountMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\DuplicateCountMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\NullCountMetric;
use Keboola\StorageDriver\BigQuery\Profile\ColumnCountMetric;
use Keboola\StorageDriver\BigQuery\Profile\DataSizeMetric;
use Keboola\StorageDriver\BigQuery\Profile\RowCountMetric;
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

        $bigQuery = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);
        /** @var string $datasetName */
        $datasetName = $command->getPath()[0]; // bucket name
        $dateset = $bigQuery->dataset($datasetName);
        $table = $dateset->table($command->getTableName());

        $response = (new CreateProfileTableResponse())
            ->setPath($command->getPath())
            ->setTableName($command->getTableName());

        $tableProfile = [];
        $tableMetrics = [
            new RowCountMetric(),
            new ColumnCountMetric(),
            new DataSizeMetric(),
        ];

        foreach ($tableMetrics as $metric) {
            $tableProfile[$metric->name()] = $metric->collect($table, $bigQuery);
        }

        $response->setProfile(json_encode($tableProfile, JSON_THROW_ON_ERROR));

        $columnProfiles = [];
        $columnMetrics = [
            new DistinctCountMetric(),
            new DuplicateCountMetric(),
            new NullCountMetric(),
        ];

        $columns = $table->info()['schema']['fields'];
        foreach ($columns as $column) {
            $columnProfile = [];

            foreach ($columnMetrics as $metric) {
                $columnProfile[$metric->name()] = $metric->collect($column['name'], $table, $bigQuery);
            }

            $columnProfiles[] = (new Column())
                ->setName($column['name'])
                ->setProfile(json_encode($columnProfile, JSON_THROW_ON_ERROR));
        }

        $response->setColumns($columnProfiles);

        return $response;
    }
}
