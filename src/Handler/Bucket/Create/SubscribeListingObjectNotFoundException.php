<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Create;

use Google\ApiCore\ApiException;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class SubscribeListingObjectNotFoundException extends Exception implements NonRetryableExceptionInterface
{
    public function __construct(
        string $message,
        int $code = self::ERR_NOT_FOUND,
        ?Throwable $previous = null
    ) {
        parent::__construct(
            $message,
            $code,
            $previous
        );
    }

    public static function handleException(ApiException $e): ApiException|self
    {
        throw new self(
            message: DecodeErrorMessage::getErrorMessage($e),
            previous: $e
        );
    }
}
