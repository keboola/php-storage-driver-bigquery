<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Project\Drop;

use Exception;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

class DropProjectHandler implements DriverCommandHandlerInterface
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

        $serviceAccountsInProject = $serviceAccountsService->listProjectsServiceAccounts(sprintf("projects/%s", $command->getProjectUserName()));
        foreach ($serviceAccountsInProject as $item) {
            $serviceAccountsService->delete(sprintf("projects/%s/serviceAccounts/%s", $command->getProjectUserName(), $item->getEmail()));
        }

        $projectsClient = $this->clientManager->getProjectClient($credentials);

        $formattedName = $projectsClient->projectName($command->getProjectUserName());
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
