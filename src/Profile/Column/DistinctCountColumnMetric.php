<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile\Column;

use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class DistinctCountColumnMetric implements ColumnMetricInterface
{
    public function name(): string
    {
        return 'distinctCount';
    }

    public function description(): string
    {
        return 'Number of distinct values in the column.';
    }

    public function collect(
        string $dataset,
        string $table,
        string $column,
        BigQueryContext $context,
    ): int {
        $sql = sprintf(
            <<<'SQL'
                SELECT COUNT(DISTINCT %s) as distinct_count FROM %s.%s
                SQL,
            BigqueryQuote::quoteSingleIdentifier($column),
            BigqueryQuote::quoteSingleIdentifier($dataset),
            BigqueryQuote::quoteSingleIdentifier($table),
        );

        /** @var array{0: array{distinct_count: int}} $results */
        $results = iterator_to_array($context->client->runQuery($context->client->query($sql)));

        return $results[0]['distinct_count'];
    }
}
