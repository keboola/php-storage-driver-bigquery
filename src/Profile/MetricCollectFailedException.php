<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Profile;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

final class MetricCollectFailedException extends Exception implements NonRetryableExceptionInterface
{
    public static function fromTableMetric(
        string $dataset,
        string $table,
        TableMetricInterface $metric,
        ?Throwable $previous = null,
    ): self {
        return new self(
            sprintf(
                'Collecting metric "%s" for table "%s.%s" failed.',
                $metric->name(),
                $dataset,
                $table,
            ),
            self::ERR_UNKNOWN,
            $previous,
        );
    }

    public static function fromColumnMetric(
        string $dataset,
        string $table,
        string $column,
        ColumnMetricInterface $metric,
        ?Throwable $previous = null,
    ): self {
        return new self(
            sprintf(
                'Collecting metric "%s" for column "%s" in table "%s.%s" failed.',
                $metric->name(),
                $column,
                $dataset,
                $table,
            ),
            self::ERR_UNKNOWN,
            $previous,
        );
    }
}
