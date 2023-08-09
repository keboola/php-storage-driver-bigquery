<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use LogicException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\BackOff\FixedBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

class BigQueryClientHandler
{
    private Client $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    public function __invoke(RequestInterface $request): ResponseInterface
    {
        $retryPolicy = new SimpleRetryPolicy(5);
        $proxy = new RetryProxy($retryPolicy, new FixedBackOffPolicy());

        /** @var ResponseInterface $result */
        $result = $proxy->call(function () use ($request) {
            return $this->client->send($request);
        });
        return $result;
    }
}
