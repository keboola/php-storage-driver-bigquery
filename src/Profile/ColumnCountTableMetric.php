<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile;

final class ColumnCountTableMetric implements TableMetricInterface
{
    public function name(): string
    {
        return 'columnCount';
    }

    public function description(): string
    {
        return 'Number of columns in the table.';
    }

    public function collect(
        string $dataset,
        string $table,
        BigQueryContext $context,
    ): int {
        return count($context->table->info()['schema']['fields']);
    }
}
