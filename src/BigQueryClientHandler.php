<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Service\Exception as GoogleServiceException;
use Google\Task\Runner;
use GuzzleHttp\Client;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Retry\BackOff\UniformRandomBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

class BigQueryClientHandler
{
    private const RETRY_MISSING_CREATE_JOB = 'bigquery.jobs.create';
    private const RETRY_SERVICE_ACCOUNT_NOT_EXIST = 'IAM setPolicy failed for Dataset';

    private Client $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    public function __invoke(RequestInterface $request): ResponseInterface
    {
        $retryCount = 0;
        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) use (&$retryCount) {
            $retryCount++;
            if ($e instanceof GoogleServiceException) {
                $retryStrategy = GCPClientManager::RETRY_MAP[$e->getCode()] ?? Runner::TASK_RETRY_NEVER;

                if (!empty($e->getErrors())) {
                    if (array_key_exists($e->getErrors()[0]['reason'], GCPClientManager::RETRY_MAP)) {
                        $retryStrategy = GCPClientManager::RETRY_MAP[$e->getErrors()[0]['reason']];
                    } elseif (str_contains($e->getErrors()[0]['message'], self::RETRY_MISSING_CREATE_JOB)) {
                        $retryStrategy = Runner::TASK_RETRY_ALWAYS;
                    } elseif (str_contains($e->getErrors()[0]['message'], self::RETRY_SERVICE_ACCOUNT_NOT_EXIST)) {
                        $retryStrategy = Runner::TASK_RETRY_ALWAYS;
                    }
                }
                return match ($retryStrategy) {
                    Runner::TASK_RETRY_ALWAYS => true,
                    Runner::TASK_RETRY_ONCE => $retryCount === 1,
                    default => false
                };
            }
            return false;
        }, 20);
        $proxy = new RetryProxy($retryPolicy, new UniformRandomBackOffPolicy());

        /** @var ResponseInterface $result */
        $result = $proxy->call(function () use ($request) {
            return $this->client->send($request);
        });
        return $result;
    }
}
