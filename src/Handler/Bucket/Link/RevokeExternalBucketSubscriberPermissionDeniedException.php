<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Link;

use Google\ApiCore\ApiException;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class RevokeExternalBucketSubscriberPermissionDeniedException extends Exception implements
    NonRetryableExceptionInterface
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

    public static function fromApiException(ApiException $e, string $listingName): self
    {
        return new self(
            message: sprintf(
                'Permission denied when revoking subscriber access on listing "%s". Assign ' .
                'listingAdmin or custom (with setIamPolicy) role to the service account and try again.',
                $listingName,
            ),
            previous: $e,
        );
    }
}
