<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;

final class ColumnCountMetric implements TableMetric
{
    public function name(): string
    {
        return 'columnCount';
    }

    public function collect(
        Table $table,
        BigQueryClient $client,
    ): int {
        return count($table->info()['schema']['fields']);
    }
}
