<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Job;
use Google\Cloud\BigQuery\JobConfigurationInterface;
use Google\Cloud\BigQuery\QueryJobConfiguration;
use Google\Cloud\BigQuery\QueryResults;

class BigQueryClientWrapper extends BigQueryClient
{
    /**
     * @inheritdoc
     * @param array<mixed> $config
     */
    public function __construct(
        readonly string $runId,
        array $config = [],
    ) {
        parent::__construct($config);
    }

    /**
     * @param array<mixed> $options
     */
    public function runQuery(JobConfigurationInterface $query, array $options = []): QueryResults
    {
        if ($this->runId !== '') {
            /** @var QueryJobConfiguration $query */
            $query = $query->labels(['run_id' => $this->runId]);
        }
        $options = array_merge($options, ['maxRetries' => 100]);
        return parent::runQuery($query, $options);
    }

    /**
     * @param QueryJobConfiguration $config
     * @param array<mixed> $options
     */
    public function runJob(JobConfigurationInterface $config, array $options = []): Job
    {
        if ($this->runId !== '') {
            $config = $config->labels(['run_id' => $this->runId]);
        }
        return parent::runJob($config, $options);
    }
}
