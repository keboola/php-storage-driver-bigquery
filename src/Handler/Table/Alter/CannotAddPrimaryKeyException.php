<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Alter;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class CannotAddPrimaryKeyException extends Exception implements NonRetryableExceptionInterface
{
    public function __construct(
        string $message,
        int $code = self::ERR_VALIDATION,
        ?Throwable $previous = null,
    ) {
        parent::__construct(
            $message,
            $code,
            $previous,
        );
    }

    public static function createForNullableColumn(string $columnName): self
    {
        return new self(sprintf('Selected column "%s" is nullable', $columnName));
    }

    public static function createForDuplicates(): self
    {
        return new self('Selected columns contain duplicities');
    }

    public static function createForExistingPK(): self
    {
        return new self('Primary key already exists');
    }
}
