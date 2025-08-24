<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile\Column;

use Google\Cloud\BigQuery\Numeric;
use Google\Cloud\Core\Exception\BadRequestException;
use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\BigQuery\Profile\MetricCollectFailedException;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class NumericStatisticsColumnMetric implements ColumnMetricInterface
{
    public function name(): string
    {
        return 'numericStatistics';
    }

    public function description(): string
    {
        return 'Basic statistics for numeric column (average, mode, median minimum, and maximum.';
    }

    /**
     * @return array{
     *     avg: float,
     *     mode: float,
     *     median: float,
     *     min: float,
     *     max: float,
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
                WITH filtered AS (
                    SELECT %s
                    FROM %s.%s
                    WHERE %s IS NOT NULL
                )
                SELECT
                    ROUND(AVG(%s), 6) AS stats_avg,
                    APPROX_TOP_COUNT(%s, 1)[OFFSET(0)].value AS stats_mode,
                    (
                        SELECT m
                        FROM (
                            SELECT PERCENTILE_CONT(%s, 0.5) OVER() AS m
                            FROM filtered
                        )
                        LIMIT 1
                    ) AS stats_median,
                    MIN(%s) AS stats_min,
                    MAX(%s) AS stats_max
                FROM filtered;
                SQL,
            $columnQuoted,
            BigqueryQuote::quoteSingleIdentifier($dataset),
            BigqueryQuote::quoteSingleIdentifier($table),
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
            $columnQuoted,
        );

        try {
            /**
             * @var array{
             *     0: array{
             *         stats_avg: float|int|Numeric,
             *         stats_mode: float|int|Numeric,
             *         stats_median: float|int|Numeric,
             *         stats_min: float|int|Numeric,
             *         stats_max: float|int|Numeric,
             *     }
             * } $results
             */
            $results = iterator_to_array($context->client->runQuery($context->client->query($sql)));
        } catch (BadRequestException $e) {
            throw MetricCollectFailedException::fromColumnMetric($dataset, $table, $column, $this, $e);
        }

        return [
            'avg' => $this->toFloat($results[0]['stats_avg']),
            'mode' => $this->toFloat($results[0]['stats_mode']),
            'median' => $this->toFloat($results[0]['stats_median']),
            'min' => $this->toFloat($results[0]['stats_min']),
            'max' => $this->toFloat($results[0]['stats_max']),
        ];
    }

    private function toFloat(float|int|Numeric $value): float
    {
        if ($value instanceof Numeric) {
            return (float) $value->get();
        }

        return (float) $value;
    }
}
