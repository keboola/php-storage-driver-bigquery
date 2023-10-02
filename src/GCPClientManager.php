<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Billing\V1\CloudBillingClient;
use Google\Cloud\ResourceManager\V3\FoldersClient;
use Google\Cloud\ResourceManager\V3\ProjectsClient;
use Google\Cloud\ServiceUsage\V1\ServiceUsageClient;
use Google\Cloud\Storage\StorageClient;
use Google\Service\Iam;
use Google_Client;
use Google_Service_CloudResourceManager;
use Google_Service_Iam;
use GuzzleHttp\Client;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

class GCPClientManager
{
    public const DEFAULT_LOCATION = 'US';

    public const SCOPES_CLOUD_PLATFORM = 'https://www.googleapis.com/auth/cloud-platform';

    /** @var array<FoldersClient|ProjectsClient|ServiceUsageClient|AnalyticsHubServiceClient> */
    private array $clients = [];

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

    public function getIamClient(GenericBackendCredentials $credentials): Iam
    {
        $client = new Google_Client([
            'credentials' => CredentialsHelper::getCredentialsArray($credentials),
        ]);
        $client->setScopes(self::SCOPES_CLOUD_PLATFORM);

        // note: the close method is not used in this client
        return new Iam($client);
    }

    public function getCloudResourceManager(GenericBackendCredentials $credentials): Google_Service_CloudResourceManager
    {
        $client = new Google_Client([
            'credentials' => CredentialsHelper::getCredentialsArray($credentials),
        ]);
        $client->setScopes(self::SCOPES_CLOUD_PLATFORM);

        // note: the close method is not used in this client
        return new Google_Service_CloudResourceManager($client);
    }

    public function getBigQueryClient(string $runId, GenericBackendCredentials $credentials): BigQueryClient
    {
        $handler = new BigQueryClientHandler(new Client());
        // note: the close method is not used in this client
        return new BigQueryClientWrapper($runId, [
            'keyFile' => CredentialsHelper::getCredentialsArray($credentials),
            'httpHandler' => $handler,
        ]);
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
}
