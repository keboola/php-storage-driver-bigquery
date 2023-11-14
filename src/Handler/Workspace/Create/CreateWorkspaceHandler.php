<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Create;

use Google\Protobuf\Internal\Message;
use Google\Service\CloudResourceManager\Binding;
use Google\Service\CloudResourceManager\Expr;
use Google\Service\CloudResourceManager\GetIamPolicyRequest;
use Google\Service\CloudResourceManager\Policy;
use Google\Service\CloudResourceManager\SetIamPolicyRequest;
use Google\Service\CloudResourceManager\TagKey;
use Google\Service\Iam\ServiceAccount;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Psr\Log\LogLevel;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

final class CreateWorkspaceHandler extends BaseHandler
{
    public const IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES = [
        IAmPermissions::ROLES_BIGQUERY_DATA_VIEWER, // readOnly access
        IAmPermissions::ROLES_BIGQUERY_JOB_USER,
        IAmPermissions::ROLES_BIGQUERY_READ_SESSION_USER,
    ];
    public const PRIVATE_KEY_TYPE = 'TYPE_GOOGLE_CREDENTIALS_FILE';
    public const KEY_DATA_PROPERTY_PRIVATE_KEY = 'private_key';

    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateWorkspaceCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateWorkspaceCommand);

        assert($runtimeOptions->getMeta() === null);

        // validate
        assert($command->getStackPrefix() !== '', 'CreateWorkspaceCommand.stackPrefix is required');
        assert($command->getWorkspaceId() !== '', 'CreateWorkspaceCommand.workspaceId is required');
        assert(
            $command->getProjectReadOnlyRoleName() !== '',
            'CreateWorkspaceCommand.projectReadOnlyRoleName is required',
        );

        $bqClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);
        $projectCredentials = CredentialsHelper::getCredentialsArray($credentials);

        $nameGenerator = new NameGenerator($command->getStackPrefix());
        $newWsDatasetName = $nameGenerator->createWorkspaceObjectNameForWorkspaceId($command->getWorkspaceId());
        $newWsServiceAccId = $nameGenerator->createWorkspaceUserNameForWorkspaceId($command->getWorkspaceId());

        // create WS service acc
        $iamService = $this->clientManager->getIamClient($credentials);
        $projectName = 'projects/' . $projectCredentials['project_id'];
        $newServiceAccountName = sprintf(
            '%s/serviceAccounts/%s@%s.iam.gserviceaccount.com',
            $projectName,
            $newWsServiceAccId,
            $projectCredentials['project_id']
        );

        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            $this->logger->debug('Try create SA Err:' . $e->getMessage());
            return true;
        }, 5);
        $backOffPolicy = new ExponentialRandomBackOffPolicy(
            5_000, // 5s
            1.8,
            60_000 // 1m
        );
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $wsServiceAcc = $proxy->call(function () use (
            $iamService,
            $newServiceAccountName,
            $newWsServiceAccId,
            $projectName
        ): ServiceAccount {
            try {
                return $iamService->projects_serviceAccounts->get($newServiceAccountName);
            } catch (Throwable) {
                $iamService->createServiceAccount($newWsServiceAccId, $projectName);
            }
            return $iamService->projects_serviceAccounts->get($newServiceAccountName);
        });
        assert($wsServiceAcc instanceof ServiceAccount);

        // create WS and grant WS service acc
        $dataset = $bqClient->createDataset($newWsDatasetName, [
            'access' => [
                'role' => IAmPermissions::ROLES_BIGQUERY_DATA_OWNER,
                'userByEmail' => $wsServiceAcc->getEmail(),
            ],
        ]);

        // grant ROLES_BIGQUERY_JOB_USER to WS service acc
        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            $this->logger->debug('Try set iam policy Err:' . $e->getMessage());
            return true;
        }, 10);
        $backOffPolicy = new ExponentialRandomBackOffPolicy(
            5_000, // 5s
            1.8,
            60_000 // 1m
        );
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);

        $wsTag = new TagKey();
        $wsTag->setParent($projectName);
        $wsTag->setShortName('workspace');

        $tags = $cloudResourceManager->tagKeys->listTagKeys([
            'parent' => 'projects/' . $projectCredentials['project_id'],
        ]);

        if (count($tags) === 0) {
            $cloudResourceManager->tagKeys->create($wsTag);
        }
        
        $tags = $cloudResourceManager->tagKeys->listTagKeys([
            'parent' => 'projects/' . $projectCredentials['project_id'],
        ]);
        $wsTagId = null;
        foreach ($tags->getTagKeys() as $tagKey) {
            if($tagKey->getShortName() === 'workspace') {
                $wsTagId = $tagKey->getName();
            }
        }
        $proxy->call(function () use ($cloudResourceManager, $projectName, $wsServiceAcc, $wsTagId): void {
            $getIamPolicyRequest = new GetIamPolicyRequest();
            $actualPolicy = $cloudResourceManager->projects->getIamPolicy($projectName, $getIamPolicyRequest, []);
            $finalBinding[] = $actualPolicy->getBindings();

            foreach (self::IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES as $role) {
                $bigQueryJobUserBinding = new Binding();
                $bigQueryJobUserBinding->setMembers('serviceAccount:' . $wsServiceAcc->getEmail());
                $bigQueryJobUserBinding->setRole($role);

                if ($role === IAmPermissions::ROLES_BIGQUERY_DATA_VIEWER) {
                    $expr = new Expr();
                    $expr->setDescription('Don\'t show other ws datasets in ws');
                    $expr->setExpression(sprintf('!resource.hasTagKeyId("%s")', $wsTagId));
                    $bigQueryJobUserBinding->setCondition($expr);
                }
                $finalBinding[] = $bigQueryJobUserBinding;
            }

            $policy = new Policy();
            $policy->setVersion($actualPolicy->getVersion());
            $policy->setEtag($actualPolicy->getEtag());
            $policy->setBindings($finalBinding);
            $setIamPolicyRequest = new SetIamPolicyRequest();
            $setIamPolicyRequest->setPolicy($policy);

            $this->logger->log(
                LogLevel::DEBUG,
                'Try set iam policy for ' . $wsServiceAcc->getEmail() . ' in ' . $projectName
            );
            $cloudResourceManager->projects->setIamPolicy($projectName, $setIamPolicyRequest);
            Helper::assertServiceAccountBindings(
                $cloudResourceManager,
                $projectName,
                $wsServiceAcc->getEmail(),
                $this->logger
            );
        });

        // generate credentials
        [$privateKey, $publicPart] = $iamService->createKeyFileCredentials($wsServiceAcc);

        return (new CreateWorkspaceResponse())
            ->setWorkspaceUserName($publicPart)
            ->setWorkspacePassword($privateKey)
            ->setWorkspaceObjectName($dataset->id());
    }
}
