<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Keboola\StorageDriver\Shared\Driver\Exception\Command\TooManyRequestsException;
use Throwable;

class ExceptionHandler
{
    public const TOO_MANY_REQUESTS_CODES = [429, 409];

    public static function handleRetryException(Throwable $e): Throwable
    {
        if (in_array($e->getCode(), self::TOO_MANY_REQUESTS_CODES)) {
            return new TooManyRequestsException();
        }
        return $e;
    }
}
