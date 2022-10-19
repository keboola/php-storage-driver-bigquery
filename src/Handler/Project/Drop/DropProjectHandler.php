<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Project\Drop;

use Exception;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class DropProjectHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropProjectCommand);

        $iamService = $this->clientManager->getIamClient($credentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;

        /** @var array<string, string>|false $publicPartKeyFile */
        $publicPartKeyFile = json_decode($command->getProjectUserName(), true, 512, JSON_THROW_ON_ERROR);
        assert($publicPartKeyFile !== false);
        $projectId = (string) $publicPartKeyFile['project_id'];
        $serviceAccountsInProject = $serviceAccountsService->listProjectsServiceAccounts(sprintf("projects/%s", $projectId));
        foreach ($serviceAccountsInProject as $item) {
            $serviceAccountsService->delete(sprintf("projects/%s/serviceAccounts/%s", $projectId, $item->getEmail()));
        }

        $projectsClient = $this->clientManager->getProjectClient($credentials);

        $formattedName = $projectsClient->projectName($projectId);
        $operationResponse = $projectsClient->deleteProject($formattedName);
        $operationResponse->pollUntilComplete();
        if (!$operationResponse->operationSucceeded()) {
            $error = $operationResponse->getError();
            assert($error !== null);
            throw new Exception($error->getMessage(), $error->getCode());
        }

        return null;
    }
}
