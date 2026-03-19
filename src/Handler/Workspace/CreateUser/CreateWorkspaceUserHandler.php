<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\CreateUser;

use Google\Protobuf\Internal\Message;
use Google\Service\CloudResourceManager\Binding;
use Google\Service\CloudResourceManager\Expr;
use Google\Service\CloudResourceManager\GetIamPolicyRequest;
use Google\Service\CloudResourceManager\GetPolicyOptions;
use Google\Service\CloudResourceManager\Policy;
use Google\Service\CloudResourceManager\SetIamPolicyRequest;
use Google\Service\Iam\ServiceAccount;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\CreateWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\Helper;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceUserCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceUserResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Psr\Log\LogLevel;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

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
        assert(
            $command->getProjectReadOnlyRoleName() !== '',
            'CreateWorkspaceUserCommand.projectReadOnlyRoleName is required',
        );

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
        $newServiceAccountName = sprintf(
            '%s/serviceAccounts/%s@%s.iam.gserviceaccount.com',
            $projectName,
            $newWsServiceAccId,
            $projectCredentials['project_id'],
        );

        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            $this->internalLogger->debug('Try create SA Err:' . $e->getMessage());
            return true;
        }, 5);
        $backOffPolicy = new ExponentialRandomBackOffPolicy(
            5_000, // 5s
            1.8,
            60_000, // 1m
        );
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $wsServiceAcc = $proxy->call(function () use (
            $iamService,
            $newServiceAccountName,
            $newWsServiceAccId,
            $projectName,
        ): ServiceAccount {
            try {
                return $iamService->projects_serviceAccounts->get($newServiceAccountName);
            } catch (Throwable) {
                $iamService->createServiceAccount($newWsServiceAccId, $projectName);
            }
            return $iamService->projects_serviceAccounts->get($newServiceAccountName);
        });
        assert($wsServiceAcc instanceof ServiceAccount);

        // grant OWNER access on the existing workspace dataset
        $dataset = $bqClient->dataset($command->getWorkspaceObjectName());
        $datasetInfo = $dataset->info();
        $access = $datasetInfo['access'] ?? [];
        $access[] = [
            'role' => IAmPermissions::ROLES_BIGQUERY_DATA_OWNER,
            'userByEmail' => $wsServiceAcc->getEmail(),
        ];
        $dataset->update(['access' => $access]);

        // grant project-level IAM roles
        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            $this->internalLogger->debug('Try set iam policy Err:' . $e->getMessage());
            return true;
        }, 10);
        $backOffPolicy = new ExponentialRandomBackOffPolicy(
            5_000, // 5s
            1.8,
            60_000, // 1m
        );
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);

        $proxy->call(function () use ($cloudResourceManager, $projectName, $wsServiceAcc): void {
            $getIamPolicyRequest = new GetIamPolicyRequest();
            $option = new GetPolicyOptions();
            $option->setRequestedPolicyVersion(Helper::REQUESTED_POLICY_VERSION);
            $getIamPolicyRequest->setOptions($option);
            $actualPolicy = $cloudResourceManager->projects->getIamPolicy($projectName, $getIamPolicyRequest);
            $finalBinding[] = $actualPolicy->getBindings();

            foreach (CreateWorkspaceHandler::IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES as $role) {
                $bigQueryJobUserBinding = new Binding();
                $bigQueryJobUserBinding->setMembers('serviceAccount:' . $wsServiceAcc->getEmail());
                $bigQueryJobUserBinding->setRole($role);

                if ($role === IAmPermissions::ROLES_BIGQUERY_DATA_VIEWER) {
                    $conditionExpression = new Expr();
                    $conditionExpression->setTitle('ReadOnly Role');
                    $conditionExpression->setDescription('Allow read only from buckets not from other ws');
                    $conditionExpression->setExpression(sprintf(
                        "!resource.name.startsWith('%s/datasets/WORKSPACE_')",
                        $projectName,
                    ));
                    $bigQueryJobUserBinding->setCondition($conditionExpression);
                }
                $finalBinding[] = $bigQueryJobUserBinding;
            }

            $policy = new Policy();
            $policy->setVersion(Helper::REQUESTED_POLICY_VERSION);
            $policy->setEtag($actualPolicy->getEtag());
            $policy->setBindings($finalBinding);
            $setIamPolicyRequest = new SetIamPolicyRequest();
            $setIamPolicyRequest->setPolicy($policy);

            $this->internalLogger->log(
                LogLevel::DEBUG,
                'Try set iam policy for ' . $wsServiceAcc->getEmail() . ' in ' . $projectName,
            );
            $cloudResourceManager->projects->setIamPolicy($projectName, $setIamPolicyRequest);
            Helper::assertServiceAccountBindings(
                $cloudResourceManager,
                $projectName,
                $wsServiceAcc->getEmail(),
                $this->internalLogger,
            );
        });

        // generate credentials
        [$privateKey, $publicPart,] = $iamService->createKeyFileCredentials($wsServiceAcc);

        return (new CreateWorkspaceUserResponse())
            ->setWorkspaceUserName($publicPart)
            ->setWorkspacePassword($privateKey);
    }
}
