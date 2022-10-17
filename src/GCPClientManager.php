<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Cloud\ResourceManager\V3\FoldersClient;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

class GCPClientManager
{
    /**
     * @throws \Google\ApiCore\ValidationException
     */
    public function getFoldersClient(GenericBackendCredentials $credentials): FoldersClient
    {
        return new FoldersClient([
            'credentials' => $this->getCredentialsArray($credentials),
        ]);
    }

    /** @return non-empty-array<string, string> */
    private function getCredentialsArray(GenericBackendCredentials $credentials): array
    {
        $credentialsArr = (array) json_decode($credentials->getPrincipal(), true, 512, JSON_THROW_ON_ERROR);
        $credentialsArr['private_key'] = $credentials->getSecret();
        return $credentialsArr;
    }
}
