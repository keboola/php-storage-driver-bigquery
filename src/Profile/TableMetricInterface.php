<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile;

interface TableMetricInterface
{
    public function name(): string;

    public function description(): string;

    /**
     * @throws MetricCollectFailedException
     * @return array<mixed>
     */
    public function collect(
        string $dataset,
        string $table,
        BigQueryContext $context,
    ): array|bool|float|int|string|null;
}
