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
        /** @var array<string, mixed> $tableInfo */
        $tableInfo = $context->table->info();
        /** @var array{fields: array<mixed>} $schema */
        $schema = $tableInfo['schema'];
        return count($schema['fields']);
    }
}
