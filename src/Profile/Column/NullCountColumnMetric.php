<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile\Column;

use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class NullCountColumnMetric implements ColumnMetricInterface
{
    public function name(): string
    {
        return 'nullCount';
    }

    public function description(): string
    {
        return 'Number of NULL values in the column.';
    }

    public function collect(
        string $dataset,
        string $table,
        string $column,
        BigQueryContext $context,
    ): int {
        $sql = sprintf(
            <<<'SQL'
                SELECT COUNT(*) as null_count FROM %s.%s WHERE %s IS NULL
                SQL,
            BigqueryQuote::quoteSingleIdentifier($dataset),
            BigqueryQuote::quoteSingleIdentifier($table),
            BigqueryQuote::quoteSingleIdentifier($column),
        );

        /** @var array{0: array{null_count: int}} $results */
        $results = iterator_to_array($context->client->runQuery($context->client->query($sql)));

        return $results[0]['null_count'];
    }
}
