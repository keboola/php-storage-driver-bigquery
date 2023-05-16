<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table;

use Google\Cloud\Core\Exception\BadRequestException;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

class BadExportFilterParameters extends Exception implements NonRetryableExceptionInterface
{
    public function __construct(string $message, int $code = self::ERR_VALIDATION, ?Throwable $previous = null)
    {
        parent::__construct(
            $message,
            $code,
            $previous
        );
    }

    /**
     * @throws self
     */
    public static function handleWrongTypeInFilters(BadRequestException $e): void
    {
        if ($e->getCode() === 400 && str_contains($e->getMessage(), 'No matching signature for operator ')) {
            $expectedActualPattern = '/types:\s(.*?)\./';
            preg_match($expectedActualPattern, $e->getMessage(), $matches);
            assert(isset($matches[1]));
            $expected = trim(explode(',', $matches[1])[0]);
            $actual = trim(explode(',', $matches[1])[1]);

            throw new self(
                message: sprintf('Invalid filter value, expected:"%s", actual:"%s".', $expected, $actual),
                previous: $e
            );
        }

        if ($e->getCode() === 400 && str_contains($e->getMessage(), 'Invalid')) {
            /** @var array<string, array<string, string>> $message */
            $message = json_decode($e->getMessage(), true);
            assert($message !== null);
            assert(isset($message['error']['message']));
            throw new self(
                message: $message['error']['message'],
                previous: $e
            );
        }
    }
}
