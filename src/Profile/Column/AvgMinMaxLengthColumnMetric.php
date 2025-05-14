<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile\Column;

use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class AvgMinMaxLengthColumnMetric implements ColumnMetricInterface
{
    public function name(): string
    {
        return 'length';
    }

    public function description(): string
    {
        return 'Average, minimum, and maximum length of strings in the column.';
    }

    /**
     * @return array{
     *     avg: float,
     *     min: int,
     *     max: int,
     * }
     */
    public function collect(
        string $dataset,
        string $table,
        string $column,
        BigQueryContext $context,
    ): array {
        $columnQuoted = BigqueryQuote::quoteSingleIdentifier($column);

        $sql = sprintf(
            <<<'SQL'
                SELECT
                    ROUND(AVG(LENGTH(%s)), 4) AS avg_length,
                    MIN(LENGTH(%s)) AS min_length,
                    MAX(LENGTH(%s)) AS max_length
                FROM %s.%s
                WHERE %s IS NOT NULL
                SQL,
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
            BigqueryQuote::quoteSingleIdentifier($dataset),
            BigqueryQuote::quoteSingleIdentifier($table),
            $columnQuoted,
        );

        /** @var array{0: array{avg_length: float, min_length: int, max_length: int}} $results */
        $results = iterator_to_array($context->client->runQuery($context->client->query($sql)));

        return [
            'avg' => $results[0]['avg_length'],
            'min' => $results[0]['min_length'],
            'max' => $results[0]['max_length'],
        ];
    }
}
