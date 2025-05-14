<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;

interface TableMetric
{
    public function name(): string;

    /**
     * @return array<mixed>
     */
    public function collect(
        Table $table,
        BigQueryClient $client,
    ): array|bool|float|int|string|null;
}
