<?php

namespace Keboola\StorageDriver\BigQuery\Handler\Backend\Init;

use Keboola\StorageDriver\Credentials\BigQueryCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Google\Cloud\ResourceManager\V3\FoldersClient;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\InitBackendResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;

final class InitBackendHandler implements DriverCommandHandlerInterface
{
    /**
     * @inheritDoc
     * @param BigQueryCredentials $credentials
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof BigQueryCredentials);
        assert($command instanceof InitBackendCommand);

        $foldersClient = new FoldersClient([
            'credentials' => [
                'type' =>$credentials->getType(),
                'project_id' =>$credentials->getProjectId(),
                'private_key_id' =>$credentials->getPrivateKeyId(),
                'private_key' =>$credentials->getPrivateKey(),
                'client_email' =>$credentials->getClientEmail(),
                'client_id' =>$credentials->getClientId(),
                'auth_uri' =>$credentials->getAuthUri(),
                'token_uri' =>$credentials->getTokenUri(),
                'auth_provider_x509_cert_url' =>$credentials->getAuthProviderX509CertUrl(),
                'client_x509_cert_url' =>$credentials->getClientX509CertUrl(),
            ],
        ]);

        try {
            $formattedName = $foldersClient->folderName($credentials->getFolderId());
            $folderPermissions = $foldersClient->testIamPermissions($formattedName, ['resourcemanager.projects.create']);
        } finally {
            $foldersClient->close();
        }

        if (count($folderPermissions->getPermissions()) === 0) {
            throw new Exception('Missing rights "resourcemanager.projects.create" for service account.');
        }

        return new InitBackendResponse();
    }
}
