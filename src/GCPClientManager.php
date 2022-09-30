<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Cloud\ResourceManager\V3\FoldersClient;
use Keboola\StorageDriver\Credentials\BigQueryCredentials;

class GCPClientManager
{
    /**
     * @throws \Google\ApiCore\ValidationException
     */
    public function getFoldersClient(BigQueryCredentials $credentials): FoldersClient
    {
        return new FoldersClient([
            'credentials' => $this->getCredentialsArray($credentials),
        ]);
    }

    /** @return non-empty-array<string, string> */
    private function getCredentialsArray(BigQueryCredentials $credentials): array
    {
        return [
            'type' => $credentials->getType(),
            'project_id' => $credentials->getProjectId(),
            'private_key_id' => $credentials->getPrivateKeyId(),
            'private_key' => $credentials->getPrivateKey(),
            'client_email' => $credentials->getClientEmail(),
            'client_id' => $credentials->getClientId(),
            'auth_uri' => $credentials->getAuthUri(),
            'token_uri' => $credentials->getTokenUri(),
            'auth_provider_x509_cert_url' => $credentials->getAuthProviderX509CertUrl(),
            'client_x509_cert_url' => $credentials->getClientX509CertUrl(),
        ];
    }
}
