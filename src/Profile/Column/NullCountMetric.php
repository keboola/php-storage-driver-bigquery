<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile\Column;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetric;

final class NullCountMetric implements ColumnMetric
{
    public function name(): string
    {
        return 'nullCount';
    }

    public function collect(
        string $columnName,
        Table $table,
        BigQueryClient $client,
    ): int {
        $tableName = $table->identity()['datasetId'] . '.' . $table->identity()['tableId'];

        $sql = sprintf(
            <<<'SQL'
                SELECT COUNT(*) as count FROM `%s` WHERE %s IS NULL
                SQL,
            $tableName,
            $columnName,
        );

        /** @var array{0: array{count: int}} $results */
        $results = iterator_to_array($client->runQuery($client->query($sql)));

        return $results[0]['count'];
    }
}
