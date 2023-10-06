<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Create;

use Google\Cloud\Core\Exception\BadRequestException;
use JsonException;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;
use const JSON_PRETTY_PRINT;

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

    /**
     * @param array<mixed> $createTableOptions
     */
    public static function handleBadRequestException(
        BadRequestException $e,
        string $datasetName,
        string $tableName,
        array $createTableOptions
    ): self {
        $message = $e->getMessage();
        try {
            $decodedMessage = json_decode($message, true, 512, JSON_THROW_ON_ERROR);
            if (is_array($decodedMessage)
                && array_key_exists('error', $decodedMessage)
                && array_key_exists('message', $decodedMessage['error'])
            ) {
                $message = $decodedMessage['error']['message'];
            }
        } catch (JsonException $e) {
            // ignore error let message as it is
        }
        throw new self(
            message: sprintf(
                'Failed to create table "%s" in dataset "%s". Exception: %s. Requested table: %s.',
                $tableName,
                $datasetName,
                $message,
                json_encode($createTableOptions, JSON_PRETTY_PRINT)
            ),
            previous: $e
        );
    }
}
