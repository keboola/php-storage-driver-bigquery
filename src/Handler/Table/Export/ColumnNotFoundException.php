<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Export;

use Google\Cloud\Core\Exception\BadRequestException;
use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

final class ColumnNotFoundException extends Exception implements
    ApplicationExceptionInterface,
    NonRetryableExceptionInterface
{
    public function __construct(string $message, int $code = self::ERR_VALIDATION, ?Throwable $previous = null)
    {
        parent::__construct(
            $message,
            $code,
            $previous,
        );
    }

    public static function handle(BadRequestException $e): void
    {
        $pattern = '/Name.* not found inside /';
        if (preg_match($pattern, $e->getMessage())) {
            throw new self(
                message: DecodeErrorMessage::getErrorMessage($e),
                previous: $e,
            );
        }
    }
}
