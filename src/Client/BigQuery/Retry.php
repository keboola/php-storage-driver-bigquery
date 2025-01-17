<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Client\BigQuery;

use GuzzleHttp\Exception\RequestException;
use JsonException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Throwable;

final class Retry
{
    private const RETRY_MISSING_CREATE_JOB = 'bigquery.jobs.create';
    private const RETRY_SERVICE_ACCOUNT_NOT_EXIST = 'IAM setPolicy failed for Dataset';
    private const RETRY_ON_REASON = [
        'rateLimitExceeded',
        'userRateLimitExceeded',
        'backendError',
        'jobRateLimitExceeded',
    ];

    public static function getRetryDecider(LoggerInterface $logger): callable
    {
        return static function (Throwable $ex) use ($logger): bool {
            $statusCode = $ex->getCode();

            if (in_array($statusCode, [429, 500, 503, 401])) {
                Retry::logRetry($statusCode, [], $logger);
                return true;
            }
            if ($statusCode >= 200 && $statusCode < 300) {
                return false;
            }

            $message = $ex->getMessage();
            if ($ex instanceof RequestException && $ex->hasResponse()) {
                $message = (string) $ex->getResponse()?->getBody();
            }
            if (str_contains($message, self::RETRY_SERVICE_ACCOUNT_NOT_EXIST)) {
                Retry::logRetry($statusCode, [$message], $logger);
                return true;
            }
            if (str_contains($message, self::RETRY_MISSING_CREATE_JOB)) {
                Retry::logRetry($statusCode, $message, $logger);
                return true;
            }

            try {
                $message = json_decode($message, true, 512, JSON_THROW_ON_ERROR);
                assert(is_array($message));
            } catch (JsonException) {
                Retry::logNotRetry($statusCode, $message, $logger);
                return false;
            }

            if (!array_key_exists('error', $message)) {
                Retry::logNotRetry($statusCode, $message, $logger);
                return false;
            }

            if (!array_key_exists('errors', $message['error'])) {
                Retry::logNotRetry($statusCode, $message, $logger);
                return false;
            }

            if (!is_array($message['error']['errors'])) {
                Retry::logNotRetry($statusCode, $message, $logger);
                return false;
            }

            foreach ($message['error']['errors'] as $error) {
                if (array_key_exists('reason', $error) && in_array($error['reason'], self::RETRY_ON_REASON, false)) {
                    Retry::logRetry($statusCode, $message, $logger);
                    return true;
                }
            }

            Retry::logNotRetry($statusCode, $message, $logger);

            return false;
        };
    }

    /**
     * @param array<mixed> $message
     * @throws JsonException
     */
    private static function logRetry(int $statusCode, array|string $message, LoggerInterface $logger): void
    {
        if (is_array($message)) {
            $message = json_encode($message, JSON_THROW_ON_ERROR);
        }

        $logger->log(
            LogLevel::INFO,
            sprintf(
                'Retrying [%s] request with exception::%s',
                $statusCode,
                $message,
            ),
        );
    }

    /**
     * @param array<mixed> $message
     * @throws JsonException
     */
    private static function logNotRetry(int $statusCode, string|array $message, LoggerInterface $logger): void
    {
        if (is_array($message)) {
            $message = json_encode($message, JSON_THROW_ON_ERROR);
        }
        $logger->log(
            LogLevel::INFO,
            sprintf(
                'Not retrying [%s] request with exception::%s',
                $statusCode,
                $message,
            ),
        );
    }
}
