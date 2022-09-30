<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Backend\Init;

use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\Credentials\BigQueryCredentials;
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
     * @param BigQueryCredentials $credentials
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof BigQueryCredentials);
        assert($command instanceof InitBackendCommand);

        $foldersClient = $this->clientManager->getFoldersClient($credentials);

        try {
            $formattedName = $foldersClient->folderName($credentials->getFolderId());
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
