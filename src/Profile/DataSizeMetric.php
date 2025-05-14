<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;

final class DataSizeMetric implements TableMetric
{
    public function name(): string
    {
        return 'dataSize';
    }

    public function collect(
        Table $table,
        BigQueryClient $client,
    ): int {
        return (int) $table->info()['numBytes'];
    }
}
