<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile;

interface ColumnMetricInterface
{
    public function name(): string;

    public function description(): string;

    /**
     * @return array<mixed>
     */
    public function collect(
        string $dataset,
        string $table,
        string $column,
        BigQueryContext $context,
    ): array|bool|float|int|string|null;
}
