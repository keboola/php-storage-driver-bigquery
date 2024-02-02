<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Project\Create;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class ProjectWithProjectIdAlreadyExists extends Exception implements NonRetryableExceptionInterface
{
    public function __construct(
        string $message,
        int $code = self::ERR_OBJECT_ALREADY_EXISTS,
        ?Throwable $previous = null,
    ) {
        parent::__construct(
            $message,
            $code,
            $previous,
        );
    }
}
