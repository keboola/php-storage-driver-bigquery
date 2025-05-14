<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile;

final class DataSizeTableMetric implements TableMetricInterface
{
    public function name(): string
    {
        return 'dataSize';
    }

    public function description(): string
    {
        return 'Allocated size of the table in bytes.';
    }

    public function collect(
        string $dataset,
        string $table,
        BigQueryContext $context,
    ): int {
        return (int) $context->table->info()['numBytes'];
    }
}
