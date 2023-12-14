<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table;

use Google\Cloud\Core\Exception\ConflictException;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class ObjectAlreadyExistsException extends Exception implements NonRetryableExceptionInterface
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

    public static function handleConflictException(ConflictException $e): ConflictException|self
    {
        if ($e->getCode() === 409) {
            throw new self(
                message: 'Object already exists.',
                previous: $e,
            );
        }

        throw $e;
    }
}
