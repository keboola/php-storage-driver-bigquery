<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Backend\Init;

use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\InitBackendResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;

final class InitBackendHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof InitBackendCommand);

        $foldersClient = $this->clientManager->getFoldersClient($credentials);

        $meta = $credentials->getMeta();
        if ($meta !== null) {
            // override root user and use other database as root
            $meta = $meta->unpack();
            assert($meta instanceof GenericBackendCredentials\BigQueryCredentialsMeta);
            $folderId = $meta->getFolderId();
        } else {
            throw new Exception('BigQueryCredentialsMeta is required.');
        }

        try {
            $formattedName = $foldersClient->folderName($folderId);
            $folderPermissions = $foldersClient->testIamPermissions(
                $formattedName,
                [
                    IAmPermissions::RESOURCE_MANAGER_PROJECTS_CREATE
                ]
            );
        } finally {
            $foldersClient->close();
        }

        if (count($folderPermissions->getPermissions()) === 0) {
            throw new Exception(sprintf(
                'Missing rights "%s for service account.',
                IAmPermissions::RESOURCE_MANAGER_PROJECTS_CREATE
            ));
        }

        return new InitBackendResponse();
    }
}
