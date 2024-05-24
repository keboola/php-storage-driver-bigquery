<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Import;

use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class MaximumLengthOverflowException extends Exception implements NonRetryableExceptionInterface
{
    private function __construct(string $message, int $code = self::ERR_VALIDATION, ?Throwable $previous = null)
    {
        parent::__construct(
            $message,
            $code,
            $previous,
        );
    }

    public static function handleException(BigqueryException $e): MaximumLengthOverflowException|BigqueryException
    {
        $msg = DecodeErrorMessage::getErrorMessage($e);
        if (str_contains($msg, 'has maximum length')) {
            // Field <X>: <Type>(<Length>) has maximum length <Length> but got a value with length <Length>
            return new self(message: $msg, previous: $e);
        }

        return $e;
    }
}
