<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile;

final class RowCountTableMetric implements TableMetricInterface
{
    public function name(): string
    {
        return 'rowCount';
    }

    public function description(): string
    {
        return 'Number of rows in the table.';
    }

    public function collect(
        string $dataset,
        string $table,
        BigQueryContext $context,
    ): int {
        /** @var array{numRows: int|string} $tableInfo */
        $tableInfo = $context->table->info();
        return (int) $tableInfo['numRows'];
    }
}
