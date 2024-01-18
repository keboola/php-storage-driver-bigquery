<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class ColumnsMismatchException extends Exception implements NonRetryableExceptionInterface
{
    public function __construct(string $message, int $code = self::ERR_VALIDATION, ?Throwable $previous = null)
    {
        parent::__construct(
            $message,
            $code,
            $previous,
        );
    }
}
