<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\JobConfigurationInterface;
use Google\Cloud\BigQuery\QueryJobConfiguration;
use Google\Cloud\BigQuery\QueryResults;

class CustomBigQueryClient extends BigQueryClient
{
    /**
     * @inheritdoc
     * @param array<mixed> $config
     */
    public function __construct(
        readonly string $runId,
        array $config = []
    ) {
        assert($this->runId !== '');
        parent::__construct($config);
    }

    /**
     * @inheritdoc
     * @param array<mixed> $options
     */
    public function runQuery(JobConfigurationInterface $query, array $options = []): QueryResults
    {
        /** @var QueryJobConfiguration $query */
        $query = $query->labels(['run_id' => $this->runId]);
        return parent::runQuery($query, $options);
    }
}
