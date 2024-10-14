<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Auth\HttpHandler\Guzzle6HttpHandler;
use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Billing\V1\CloudBillingClient;
use Google\Cloud\ResourceManager\V3\FoldersClient;
use Google\Cloud\ResourceManager\V3\ProjectsClient;
use Google\Cloud\ServiceUsage\V1\ServiceUsageClient;
use Google\Cloud\Storage\StorageClient;
use Google\Task\Runner;
use Google_Client;
use Google_Service_CloudResourceManager;
use GuzzleHttp\Client;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\HandlerStack;
use Keboola\StorageDriver\BigQuery\Client\BigQuery\Retry;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientWrapper;
use Psr\Log\LoggerInterface;
use Throwable;

class GCPClientManager
{
    public const CONNECTION_TIMEOUTS_MAP = [
        'timeout' => self::TIMEOUT,
        'connect_timeout' => self::CONNECT_TIMEOUT,
    ];
    private const KEBOOLA_USER_AGENT = 'Keboola/1.0 (GPN:Keboola; connection)';
    public const TIMEOUT = 120;
    public const CONNECT_TIMEOUT = 10;
    private const DEFAULT_RETRIES_COUNT = 20;
    public const RETRY_MAP = [ // extends Google\Task\Runner::$retryMap
        '500' => Runner::TASK_RETRY_ALWAYS,
        '503' => Runner::TASK_RETRY_ALWAYS,
        '400' => Runner::TASK_RETRY_ALWAYS,
        'rateLimitExceeded' => Runner::TASK_RETRY_ALWAYS,
        'accessDenied' => Runner::TASK_RETRY_ONCE,
        'userRateLimitExceeded' => Runner::TASK_RETRY_ALWAYS,
        6 => Runner::TASK_RETRY_ALWAYS,  // CURLE_COULDNT_RESOLVE_HOST
        7 => Runner::TASK_RETRY_ALWAYS,  // CURLE_COULDNT_CONNECT
        28 => Runner::TASK_RETRY_ALWAYS,  // CURLE_OPERATION_TIMEOUTED
        35 => Runner::TASK_RETRY_ALWAYS,  // CURLE_SSL_CONNECT_ERROR
        52 => Runner::TASK_RETRY_ALWAYS,  // CURLE_GOT_NOTHING
        'lighthouseError' => Runner::TASK_RETRY_NEVER,
    ];
    public const SCOPES_CLOUD_PLATFORM = 'https://www.googleapis.com/auth/cloud-platform';

    /** @var array<FoldersClient|ProjectsClient|ServiceUsageClient|AnalyticsHubServiceClient> */
    private array $clients = [];

    public const DEFAULT_RETRY_SETTINGS =
        [
            // try max 3 times
            'retries' => 3,
            // multiplicator of backoff time between runs. First = $initial_delay ; second $previousDelay * $factor
            'factor' => 1.1,
            // by default, we know that we have to wait 60 seconds
            'initial_delay' => 60,
            // randomize backoff time +/- 10%
            'jitter' => 0.1,
        ];

    public function __construct(private readonly LoggerInterface $logger)
    {
    }

    /**
     * @throws \Google\ApiCore\ValidationException
     */
    public function getFoldersClient(GenericBackendCredentials $credentials): FoldersClient
    {
        $client = new FoldersClient([
            'credentials' => CredentialsHelper::getCredentialsArray($credentials),
        ]);

        $this->clients[] = $client;

        return $client;
    }

    public function getProjectClient(GenericBackendCredentials $credentials): ProjectsClient
    {
        $client = new ProjectsClient([
            'credentials' => CredentialsHelper::getCredentialsArray($credentials),
        ]);

        $this->clients[] = $client;

        return $client;
    }

    public function getServiceUsageClient(GenericBackendCredentials $credentials): ServiceUsageClient
    {
        $client = new ServiceUsageClient([
            'credentials' => CredentialsHelper::getCredentialsArray($credentials),
        ]);

        $this->clients[] = $client;

        return $client;
    }

    public function getIamClient(GenericBackendCredentials $credentials): IAMServiceWrapper
    {
        $client = new Google_Client([
            'credentials' => CredentialsHelper::getCredentialsArray($credentials),
            'retry' => self::DEFAULT_RETRY_SETTINGS,
        ]);

        $this->setTimeouts($client);
        $client->setScopes(self::SCOPES_CLOUD_PLATFORM);

        // note: the close method is not used in this client
        return new IAMServiceWrapper($client);
    }

    public function getCloudResourceManager(GenericBackendCredentials $credentials): Google_Service_CloudResourceManager
    {
        $client = new Google_Client([
            'credentials' => CredentialsHelper::getCredentialsArray($credentials),
            'retry' => self::DEFAULT_RETRY_SETTINGS,
            'retry_map' => self::RETRY_MAP,
        ]);
        $this->setTimeouts($client);
        $client->setScopes(self::SCOPES_CLOUD_PLATFORM);

        // note: the close method is not used in this client
        return new Google_Service_CloudResourceManager($client);
    }

    /**
     * @throws CredentialsMetaRequiredException
     * @throws \JsonException
     */
    public function getBigQueryClient(
        string $runId,
        GenericBackendCredentials $credentials,
        ?HandlerStack $handlerStack = null,
        int $retries = self::DEFAULT_RETRIES_COUNT,
    ): BigQueryClient {
        $credentialsMeta = CredentialsHelper::getBigQueryCredentialsMeta($credentials);

        if ($handlerStack === null) {
            $handlerStack = HandlerStack::create();
        }
        $guzzleClient = new Client(['handler' => $handlerStack]);

        // note: the close method is not used in this client
        return new BigQueryClientWrapper([
            'keyFile' => CredentialsHelper::getCredentialsArray($credentials),
            'httpHandler' => new Guzzle6HttpHandler($guzzleClient),
            'restRetryFunction' => function () {
                // BigQuery client sometimes calls directly restRetryFunction with exception as first argument
                // But in other cases it expects to return callable which accepts exception as first argument
                $argsNum = func_num_args();
                if ($argsNum === 2) {
                    $ex = func_get_arg(0);
                    if ($ex instanceof Throwable) {
                        return Retry::getRetryDecider($this->logger)($ex);
                    }
                }
                return Retry::getRetryDecider($this->logger);
            },
            'requestTimeout' => self::TIMEOUT,
            'restOptions' => [
                'headers' => [
                    'User-Agent' => self::KEBOOLA_USER_AGENT,
                ],
            ],
            'retries' => $retries,
            'location' => $credentialsMeta->getRegion(),
        ], $runId);
    }

    public function getBillingClient(GenericBackendCredentials $credentials): CloudBillingClient
    {
        // note: the close method is not used in this client
        return new CloudBillingClient([
            'credentials' => CredentialsHelper::getCredentialsArray($credentials),
        ]);
    }

    public function getStorageClient(GenericBackendCredentials $credentials): StorageClient
    {
        // note: the close method is not used in this client
        return new StorageClient([
            'keyFile' => CredentialsHelper::getCredentialsArray($credentials),
            'requestTimeout' => self::TIMEOUT,
        ]);
    }

    public function getAnalyticHubClient(GenericBackendCredentials $credentials): AnalyticsHubServiceClient
    {
        $client = new AnalyticsHubServiceClient([
            'credentials' => CredentialsHelper::getCredentialsArray($credentials),
        ]);

        $this->clients[] = $client;

        return $client;
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close(): void
    {
        foreach ($this->clients as $client) {
            $client->close();
        }
    }

    private function setTimeouts(Google_Client $client): void
    {
        $guzzleClient = $client->getHttpClient();

        $config = $guzzleClient->getConfig();
        assert(is_array($config));
        $config = array_merge($config, self::CONNECTION_TIMEOUTS_MAP);
        $client->setHttpClient(new GuzzleClient($config));
    }
}
