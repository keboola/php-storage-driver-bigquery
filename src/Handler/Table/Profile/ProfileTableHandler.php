<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Profile;

use Google\Protobuf\Internal\Message;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\Column\AvgMinMaxLengthColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\DistinctCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\NullCountColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\Column\NumericStatisticsColumnMetric;
use Keboola\StorageDriver\BigQuery\Profile\ColumnCountTableMetric;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\BigQuery\Profile\DataSizeTableMetric;
use Keboola\StorageDriver\BigQuery\Profile\MetricCollectFailedException;
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

        /** @var array<string, string> $queryTags */
        $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

        $bigQuery = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            $queryTags,
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
            try {
                $tableProfile[$metric->name()] = $metric->collect(
                    $datasetName,
                    $command->getTableName(),
                    $bigQueryContext,
                );
            } catch (MetricCollectFailedException $e) {
                $this->internalLogger->warning(
                    $e->getMessage(),
                    [
                        'exception' => $e->getPrevious(),
                    ],
                );
            }
        }

        $response->setProfile(json_encode($tableProfile, JSON_THROW_ON_ERROR));
        $columns = $table->info()['schema']['fields'];

        $columnProfiles = [];
        foreach ($columns as $column) {
            $columnName = $column['name'];
            $columnMetrics = $this->columnMetricsByType($column['type']);

            $columnProfile = [];
            foreach ($columnMetrics as $metric) {
                try {
                    $columnProfile[$metric->name()] = $metric->collect(
                        $datasetName,
                        $command->getTableName(),
                        $columnName,
                        $bigQueryContext,
                    );
                } catch (MetricCollectFailedException $e) {
                    $this->internalLogger->warning(
                        $e->getMessage(),
                        [
                            'exception' => $e->getPrevious(),
                        ],
                    );
                }
            }

            $columnProfiles[] = (new Column())
                ->setName($columnName)
                ->setProfile(json_encode($columnProfile, JSON_THROW_ON_ERROR));
        }

        $response->setColumns($columnProfiles);

        return $response;
    }

    /**
     * @return ColumnMetricInterface[]
     */
    private function columnMetricsByType(string $type): array
    {
        $default = [
            new DistinctCountColumnMetric(),
            new DuplicateCountColumnMetric(),
            new NullCountColumnMetric(),
        ];

        try {
            $baseType = (new Bigquery($type))->getBasetype();
        } catch (InvalidTypeException) {
            $baseType = null;
        }

        $extra = match ($baseType) {
            BaseType::FLOAT,
            BaseType::INTEGER,
            BaseType::NUMERIC => [
                new NumericStatisticsColumnMetric(),
            ],
            BaseType::STRING => [
                new AvgMinMaxLengthColumnMetric(),
            ],
            default => [],
        };

        return array_merge($default, $extra);
    }
}
