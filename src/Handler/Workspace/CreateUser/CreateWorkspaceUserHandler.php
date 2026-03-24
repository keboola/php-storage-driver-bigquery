<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\CreateUser;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\Helper;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceUserCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceUserResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;

final class CreateWorkspaceUserHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateWorkspaceUserCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateWorkspaceUserCommand);

        assert($runtimeOptions->getMeta() === null);

        // validate
        assert($command->getStackPrefix() !== '', 'CreateWorkspaceUserCommand.stackPrefix is required');
        assert($command->getWorkspaceId() !== '', 'CreateWorkspaceUserCommand.workspaceId is required');
        assert($command->getWorkspaceObjectName() !== '', 'CreateWorkspaceUserCommand.workspaceObjectName is required');

        $projectCredentials = CredentialsHelper::getCredentialsArray($credentials);

        /** @var array<string, string> $queryTags */
        $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

        $bqClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            $queryTags,
        );

        $nameGenerator = new NameGenerator($command->getStackPrefix());
        $newWsServiceAccId = $nameGenerator->createWorkspaceCredentialsUserName($command->getWorkspaceId());

        // create service account
        $iamService = $this->clientManager->getIamClient($credentials);
        $projectName = 'projects/' . $projectCredentials['project_id'];

        $wsServiceAcc = Helper::createServiceAccount(
            $iamService,
            $newWsServiceAccId,
            $projectName,
            $this->internalLogger,
        );

        // grant OWNER access on the existing workspace dataset
        $dataset = $bqClient->dataset($command->getWorkspaceObjectName());
        /** @var array<string, mixed> $datasetInfo */
        $datasetInfo = $dataset->info();
        /** @var list<array<string, mixed>> $access */
        $access = $datasetInfo['access'] ?? [];
        $access[] = [
            'role' => IAmPermissions::ROLES_BIGQUERY_DATA_OWNER,
            'userByEmail' => $wsServiceAcc->getEmail(),
        ];
        $dataset->update(['access' => $access]);

        // grant project-level IAM roles
        $hasReadOnlyAccess = $command->getProjectReadOnlyRoleName() !== '';
        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        Helper::grantProjectIamRoles(
            $cloudResourceManager,
            $projectName,
            $wsServiceAcc,
            $this->internalLogger,
            $hasReadOnlyAccess,
        );

        // generate credentials
        [$privateKey, $publicPart,] = $iamService->createKeyFileCredentials($wsServiceAcc);

        return (new CreateWorkspaceUserResponse())
            ->setWorkspaceUserName($publicPart)
            ->setWorkspacePassword($privateKey);
    }
}
