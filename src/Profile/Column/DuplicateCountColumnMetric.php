<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile\Column;

use Google\Cloud\Core\Exception\BadRequestException;
use Keboola\StorageDriver\BigQuery\Profile\BigQueryContext;
use Keboola\StorageDriver\BigQuery\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\BigQuery\Profile\MetricCollectFailedException;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class DuplicateCountColumnMetric implements ColumnMetricInterface
{
    public function name(): string
    {
        return 'duplicateCount';
    }

    public function description(): string
    {
        return 'Number of duplicate values in the column.';
    }

    public function collect(
        string $dataset,
        string $table,
        string $column,
        BigQueryContext $context,
    ): int {
        $columnQuoted = BigqueryQuote::quoteSingleIdentifier($column);

        $sql = sprintf(
            <<<'SQL'
                SELECT COUNT(%s) - COUNT(DISTINCT %s) as duplicate_count FROM %s.%s WHERE %s IS NOT NULL
                SQL,
            $columnQuoted,
            $columnQuoted,
            BigqueryQuote::quoteSingleIdentifier($dataset),
            BigqueryQuote::quoteSingleIdentifier($table),
            $columnQuoted,
        );

        try {
            /** @var array{0: array{duplicate_count: int}} $results */
            $results = iterator_to_array($context->client->runQuery($context->client->query($sql)));
        } catch (BadRequestException $e) {
            throw MetricCollectFailedException::fromColumnMetric($dataset, $table, $column, $this, $e);
        }

        return $results[0]['duplicate_count'];
    }
}
