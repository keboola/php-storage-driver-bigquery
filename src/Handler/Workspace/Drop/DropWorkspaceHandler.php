<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class DropWorkspaceHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param DropWorkspaceCommand $command
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropWorkspaceCommand);

        // validate
        assert($command->getWorkspaceUserName() !== '', 'DropWorkspaceCommand.workspaceUserName is required');
        assert($command->getWorkspaceObjectName() !== '', 'DropWorkspaceCommand.workspaceObjectName is required');

        $bqClient = $this->clientManager->getBigQueryClient($credentials);
        $dataset = $bqClient->dataset($command->getWorkspaceObjectName());
        $dataset->delete(['deleteContents' => $command->getIsCascade()]);

        $iamService = $this->clientManager->getIamClient($credentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        // get info about ws service acc from ws service acc credentials
        /** @var array<string, string> $keyData */
        $keyData = json_decode($command->getWorkspaceUserName(), true, 512, JSON_THROW_ON_ERROR);
        $serviceAccountsService->delete(
            sprintf('projects/%s/serviceAccounts/%s', $keyData['project_id'], $keyData['client_email'])
        );
        return null;
    }
}
