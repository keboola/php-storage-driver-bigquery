<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Create;

use Google\ApiCore\ApiException;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class SubscribeListingPermissionDeniedException extends Exception implements NonRetryableExceptionInterface
{
    public function __construct(
        string $message,
        int $code = self::ERR_OBJECT_PERMISSION_DENIED,
        ?Throwable $previous = null,
    ) {
        parent::__construct(
            $message,
            $code,
            $previous,
        );
    }

    public static function handlePermissionDeniedException(
        ApiException $e,
        string $externalBucketName,
        string $listingName,
    ): ApiException|self {
        throw new self(
            message: sprintf(
                'Failed to register external bucket "%s" permission denied for subscribe listing "%s"',
                $externalBucketName,
                $listingName,
            ),
            previous: $e,
        );
    }
}
