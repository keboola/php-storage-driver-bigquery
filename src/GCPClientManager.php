<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Cloud\ResourceManager\V3\FoldersClient;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Google\Cloud\ResourceManager\V3\ProjectsClient;
use Google\Cloud\ServiceUsage\V1\ServiceUsageClient;
use Google_Client;
use Google_Service_CloudResourceManager;
use Google_Service_Iam;

class GCPClientManager
{
    public const SCOPES_CLOUD_PLATFORM = 'https://www.googleapis.com/auth/cloud-platform';

    /** @var array<FoldersClient|ProjectsClient|ServiceUsageClient> */
    private array $clients = [];

    /**
     * @throws \Google\ApiCore\ValidationException
     */
    public function getFoldersClient(GenericBackendCredentials $credentials): FoldersClient
    {
        $client = new FoldersClient([
            'credentials' => $this->getCredentialsArray($credentials),
        ]);
        $this->clients[] = $client;

        return $client;
    }

    public function getProjectClient(GenericBackendCredentials $credentials): ProjectsClient
    {
        $client = new ProjectsClient([
            'credentials' => $this->getCredentialsArray($credentials),
        ]);

        $this->clients[] = $client;

        return $client;
    }

    public function getServiceUsageClient(GenericBackendCredentials $credentials): ServiceUsageClient
    {
        $client = new ServiceUsageClient([
            'credentials' => $this->getCredentialsArray($credentials),
        ]);

        $this->clients[] = $client;

        return $client;
    }

    public function getIamClient(GenericBackendCredentials $credentials): Google_Service_Iam
    {
        $client = new Google_Client([
            'credentials' => $this->getCredentialsArray($credentials),
        ]);
        $client->setScopes(self::SCOPES_CLOUD_PLATFORM);

        return new Google_Service_Iam($client);
    }

    public function getCloudResourceManager(GenericBackendCredentials $credentials): Google_Service_CloudResourceManager
    {
        $client = new Google_Client([
            'credentials' => $this->getCredentialsArray($credentials),
        ]);
        $client->setScopes(self::SCOPES_CLOUD_PLATFORM);
        return new Google_Service_CloudResourceManager($client);
    }

    /** @return non-empty-array<string, string> */
    private function getCredentialsArray(GenericBackendCredentials $credentials): array
    {
        $credentialsArr = (array) json_decode($credentials->getPrincipal(), true, 512, JSON_THROW_ON_ERROR);
        $credentialsArr['private_key'] = $credentials->getSecret();
        return $credentialsArr;
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
