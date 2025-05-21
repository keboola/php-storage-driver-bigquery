<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile\Column;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetric;

final class AvgMinMaxLengthMetric implements ColumnMetric
{
    public function name(): string
    {
        return 'length';
    }

    /**
     * @return array{
     *     avg: float,
     *     min: int,
     *     max: int,
     * }
     */
    public function collect(
        string $columnName,
        Table $table,
        BigQueryClient $client,
    ): array {
        $tableName = $table->identity()['datasetId'] . '.' . $table->identity()['tableId'];

        $sql = sprintf(
            <<<'SQL'
                SELECT
                    ROUND(AVG(LENGTH(%s)), 4) AS avg,
                    MIN(LENGTH(%s)) AS min,
                    MAX(LENGTH(%s)) AS max
                FROM `%s`
                WHERE %s IS NOT NULL
                SQL,
            $columnName,
            $columnName,
            $columnName,
            $tableName,
            $columnName,
        );

        /** @var array{0: array{avg: float, min: int, max: int}} $results */
        $results = iterator_to_array($client->runQuery($client->query($sql)));

        return [
            'avg' => $results[0]['avg'],
            'min' => $results[0]['min'],
            'max' => $results[0]['max'],
        ];
    }
}
