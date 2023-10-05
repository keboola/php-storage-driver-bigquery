<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Create;

use Google\Cloud\Core\Exception\BadRequestException;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class BadTableDefinitionException extends Exception implements NonRetryableExceptionInterface
{
    public function __construct(
        string $message,
        int $code = self::ERR_VALIDATION,
        ?Throwable $previous = null
    ) {
        parent::__construct(
            $message,
            $code,
            $previous
        );
    }

    public static function handleBadRequestException(
        BadRequestException $e,
        string $datasetName,
        string $tableName
    ): self {
        throw new self(
            message: sprintf(
                'Failed to create table "%s" in dataset "%s": %s.',
                $tableName,
                $datasetName,
                $e->getMessage()
            ),
            previous: $e
        );
    }
}
