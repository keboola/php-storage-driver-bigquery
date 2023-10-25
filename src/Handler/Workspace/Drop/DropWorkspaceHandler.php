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
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

final class DropWorkspaceHandler extends BaseHandler
{
    private const ERROR_CODES_FOR_RETRY = [401, 403, 429];
    private const ERROR_CODES_FOR_RETRY_IAM = [409, ...self::ERROR_CODES_FOR_RETRY];

    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
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

        assert($runtimeOptions->getMeta() === null);

        // validate
        assert($command->getWorkspaceUserName() !== '', 'DropWorkspaceCommand.workspaceUserName is required');
        assert($command->getWorkspaceObjectName() !== '', 'DropWorkspaceCommand.workspaceObjectName is required');

        $bqClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);
        $dataset = $bqClient->dataset($command->getWorkspaceObjectName());

        $deleteWsDatasetRetryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            if (in_array($e->getCode(), self::ERROR_CODES_FOR_RETRY)) {
                return true;
            }
            return false;
        }, 20);

        $proxy = new RetryProxy($deleteWsDatasetRetryPolicy, new ExponentialRandomBackOffPolicy());
        try {
            $proxy->call(function () use ($dataset, $command): void {
                $dataset->delete(['deleteContents' => $command->getIsCascade()]);
            });
        } catch (Throwable $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
            // ignore 404 exception as dataset is probably deleted in retry
        }

        $iamService = $this->clientManager->getIamClient($credentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        // get info about ws service acc from ws service acc credentials
        /** @var array<string, string> $keyData */
        $keyData = json_decode($command->getWorkspaceUserName(), true, 512, JSON_THROW_ON_ERROR);

        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);

        $setIamPolicyRetryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            if (in_array($e->getCode(), self::ERROR_CODES_FOR_RETRY_IAM)) {
                return true;
            }
            return false;
        }, 10);
        $proxy = new RetryProxy($setIamPolicyRetryPolicy, new ExponentialRandomBackOffPolicy());
        $proxy->call(function () use ($cloudResourceManager, $credentials, $keyData): void {
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
                    if ($member !== 'serviceAccount:' . $keyData['client_email']) {
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
        });

        $deleteServiceAccRetryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            if (in_array($e->getCode(), self::ERROR_CODES_FOR_RETRY)) {
                return true;
            }
            return false;
        }, 10);

        $proxy = new RetryProxy($deleteServiceAccRetryPolicy, new ExponentialRandomBackOffPolicy());
        $proxy->call(function () use ($serviceAccountsService, $keyData): void {
            $serviceAccountsService->delete(
                sprintf('projects/%s/serviceAccounts/%s', $keyData['project_id'], $keyData['client_email'])
            );
        });

        return null;
    }
}
