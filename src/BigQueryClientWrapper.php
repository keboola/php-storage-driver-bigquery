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
        return parent::runQuery($query, $options);
    }

    public function executeQuery(QueryJobConfiguration $query): QueryResults
    {
        if ($this->runId !== '') {
            /** @var QueryJobConfiguration $query */
            $query = $query->labels(['run_id' => $this->runId]);
        }
        $job = $this->startQuery($query);

        $retriesCount = 0;
        do {
            $waitSeconds = (int) min(pow(2, $retriesCount), 20);
            sleep($waitSeconds);

            $job->reload();

            $retriesCount++;
        } while (!$job->isComplete());

        return $job->queryResults();
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
