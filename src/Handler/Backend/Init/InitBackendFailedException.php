<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Backend\Init;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class InitBackendFailedException extends Exception implements NonRetryableExceptionInterface
{
    private function __construct(string $message)
    {
        parent::__construct(
            $message,
            self::ERR_VALIDATION,
        );
    }

    /**
     * @param Throwable[] $exceptions
     * @throws self
     */
    public static function handleExceptions(array $exceptions): void
    {
        if ($exceptions === []) {
            return;
        }
        throw new self(sprintf(
            <<<EOD
'Cannot initialize backend. Following errors were encountered:
%s',
EOD,
            implode(
                PHP_EOL,
                array_map(fn(Throwable $e) => $e->getMessage(), $exceptions),
            ),
        ));
    }
}
