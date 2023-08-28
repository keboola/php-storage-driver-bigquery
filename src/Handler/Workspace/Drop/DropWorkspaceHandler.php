<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Drop;

use Google\Protobuf\Internal\Message;
use Google\Service\CloudResourceManager\Binding;
use Google\Service\CloudResourceManager\GetIamPolicyRequest;
use Google\Service\CloudResourceManager\Policy;
use Google\Service\CloudResourceManager\SetIamPolicyRequest;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
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
        array $features,
        Message $runtimeOptions
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropWorkspaceCommand);

        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

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

        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        $getIamPolicyRequest = new GetIamPolicyRequest();
        $projectCredentials = CredentialsHelper::getCredentialsArray($credentials);
        $projectName = 'projects/' . $projectCredentials['project_id'];
        $actualPolicy = $cloudResourceManager->projects->getIamPolicy($projectName, $getIamPolicyRequest);
        $actualBinding[] = $actualPolicy->getBindings();

        $newBinding = [];
        /** @var Binding $binding */
        foreach ($actualBinding[0] as $binding) {
            $tmpBinding = new Binding();
            $tmpBinding->setRole($binding->getRole());
            if ($binding->getCondition() !== null) {
                $tmpBinding->setCondition($binding->getCondition());
            }
            $newMembers = [];
            foreach ($binding->getMembers() as $member) {
                if ($member !== 'serviceAccount:'.$keyData['client_email']) {
                    $newMembers[] = $member;
                }
            }
            $tmpBinding->setMembers($newMembers);
            $newBinding[] = $tmpBinding;
        }
        $policy = new Policy();
        $policy->setBindings($newBinding);
        $setIamPolicyRequest = new SetIamPolicyRequest();
        $setIamPolicyRequest->setPolicy($policy);
        $cloudResourceManager->projects->setIamPolicy($projectName, $setIamPolicyRequest);

        $serviceAccountsService->delete(
            sprintf('projects/%s/serviceAccounts/%s', $keyData['project_id'], $keyData['client_email'])
        );
        return null;
    }
}
