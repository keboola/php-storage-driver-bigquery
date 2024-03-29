<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class CredentialsMetaRequiredException extends Exception implements NonRetryableExceptionInterface
{
    public function __construct(
        int $code = self::ERR_VALIDATION,
        ?Throwable $previous = null,
    ) {
        parent::__construct(
            'BigQueryCredentialsMeta is required.',
            $code,
            $previous,
        );
    }
}
