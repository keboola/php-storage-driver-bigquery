<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Drop;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Job;
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
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientWrapper;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

final class DropWorkspaceHandler extends BaseHandler
{
    private const ERROR_CODES_FOR_RETRY = [401, 403, 429];

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
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropWorkspaceCommand);

        assert($runtimeOptions->getMeta() === null);

        // validate
        assert($command->getWorkspaceUserName() !== '', 'DropWorkspaceCommand.workspaceUserName is required');
        assert($command->getWorkspaceObjectName() !== '', 'DropWorkspaceCommand.workspaceObjectName is required');

        $bqClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);
        // get info about ws service acc from ws service acc credentials
        /** @var array<string, string> $keyData */
        $keyData = json_decode($command->getWorkspaceUserName(), true, 512, JSON_THROW_ON_ERROR);

        $this->deleteDataset($bqClient, $command);
        $this->dropIamPolicies($credentials, $keyData['client_email']);
        $this->cancelRunningJobs($bqClient, $keyData['client_email']);
        $this->deleteServiceAccount($credentials, $keyData['project_id'], $keyData['client_email']);

        return null;
    }

    private function cancelRunningJobs(
        BigQueryClient $bqClient,
        string $serviceAccountEmail,
    ): void {
        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            if (in_array($e->getCode(), self::ERROR_CODES_FOR_RETRY)) {
                return true;
            }
            return false;
        }, 10);

        $proxy = new RetryProxy($retryPolicy, new ExponentialRandomBackOffPolicy());
        $jobs = $bqClient->jobs(
            [
                'stateFilter' => 'RUNNING',
                'allUsers' => true,
            ],
        );
        /** @var Job $job */
        foreach ($jobs as $job) {
            // Check if the job belongs to the service account we're removing
            $jobInfo = $job->reload(); // job needs to reload to fetch user_email
            if (array_key_exists('user_email', $jobInfo) && $jobInfo['user_email'] === $serviceAccountEmail) {
                try {
                    $this->userLogger->info(sprintf(
                        'Canceling job %s for service account %s',
                        $job->id(),
                        $serviceAccountEmail,
                    ));
                    $proxy->call(function () use ($job): void {
                        $job->cancel();
                    });
                } catch (Throwable $e) {
                    $this->userLogger->warning(sprintf(
                        'Could not cancel job %s for service account %s: %s',
                        $job->id(),
                        $serviceAccountEmail,
                        $e->getMessage(),
                    ));
                }
            }
        }
    }

    private function deleteDataset(
        BigQueryClient $bqClient,
        DropWorkspaceCommand $command,
    ): void {
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
    }

    private function dropIamPolicies(
        GenericBackendCredentials $credentials,
        string $serviceAccountEmail,
    ): void {
        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        $setIamPolicyRetryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            if (in_array($e->getCode(), GCPClientManager::ERROR_CODES_FOR_RETRY_IAM)) {
                return true;
            }
            return false;
        }, 20);
        $proxy = new RetryProxy($setIamPolicyRetryPolicy, new ExponentialRandomBackOffPolicy());
        $proxy->call(function () use ($cloudResourceManager, $credentials, $serviceAccountEmail): void {
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
                    if ($member !== 'serviceAccount:' . $serviceAccountEmail) {
                        $newMembers[] = $member;
                    }
                }
                $tmpBinding->setMembers($newMembers);
                $newBinding[] = $tmpBinding;
            }
            $policy = new Policy();
            $policy->setVersion($actualPolicy->getVersion());
            $policy->setEtag($actualPolicy->getEtag());
            $policy->setBindings($newBinding);
            $setIamPolicyRequest = new SetIamPolicyRequest();
            $setIamPolicyRequest->setPolicy($policy);
            $cloudResourceManager->projects->setIamPolicy($projectName, $setIamPolicyRequest);
        });
    }

    private function deleteServiceAccount(
        GenericBackendCredentials $credentials,
        string $projectId,
        string $serviceAccountEmail,
    ): void {
        $iamService = $this->clientManager->getIamClient($credentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $deleteServiceAccRetryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            if (in_array($e->getCode(), self::ERROR_CODES_FOR_RETRY)) {
                return true;
            }
            return false;
        }, 10);

        $proxy = new RetryProxy($deleteServiceAccRetryPolicy, new ExponentialRandomBackOffPolicy());
        $proxy->call(function () use ($serviceAccountsService, $projectId, $serviceAccountEmail): void {
            $serviceAccountsService->delete(
                sprintf('projects/%s/serviceAccounts/%s', $projectId, $serviceAccountEmail),
            );
        });
    }
}
