<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Project\Create;

use Google\ApiCore\ApiException;
use Google\ApiCore\ValidationException;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DataExchange;
use Google\Cloud\Billing\V1\ProjectBillingInfo;
use Google\Cloud\ResourceManager\V3\Project;
use Google\Cloud\ResourceManager\V3\ProjectsClient;
use Google\Cloud\ServiceUsage\V1\ServiceUsageClient;
use Google\Protobuf\Internal\Message;
use Google\Service\CloudResourceManager\Binding;
use Google\Service\CloudResourceManager\GetIamPolicyRequest;
use Google\Service\CloudResourceManager\Policy;
use Google\Service\CloudResourceManager\SetIamPolicyRequest;
use Google\Service\Iam\ServiceAccount;
use Google_Service_CloudResourceManager;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\GCPServiceIds;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\BigQuery\IAMServiceWrapper;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Throwable;

final class CreateProjectHandler extends BaseHandler
{
    public const ENABLED_SERVICES_FOR_PROJECT = [
        GCPServiceIds::SERVICE_USAGE_SERVICE,
        GCPServiceIds::IAM_SERVICE,
        GCPServiceIds::BIGQUERY_SERVICE,
        GCPServiceIds::CLOUD_BILLING_SERVICE,
        GCPServiceIds::CLOUD_RESOURCE_MANAGER_SERVICE,
        GCPServiceIds::CLOUD_ANALYTIC_HUB_SERVICE,
    ];

    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @throws ValidationException
     * @throws Exception
     * @throws ApiException
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateProjectCommand);

        assert($runtimeOptions->getMeta() === null);

        $nameGenerator = new NameGenerator($command->getStackPrefix());

        $projectId = $nameGenerator->createProjectId($command->getProjectId());

        $meta = $credentials->getMeta();
        if ($meta !== null) {
            // override root user and use other database as root
            $meta = $meta->unpack();
            assert($meta instanceof GenericBackendCredentials\BigQueryCredentialsMeta);
            $folderId = $meta->getFolderId();
        } else {
            throw new Exception('BigQueryCredentialsMeta is required.');
        }

        $commandMeta = $command->getMeta();
        if ($commandMeta !== null) {
            // override root user and use other database as root
            $commandMeta = $commandMeta->unpack();
            assert($commandMeta instanceof CreateProjectCommand\CreateProjectBigqueryMeta);
            $fileStorageBucketName = $commandMeta->getGcsFileBucketName();
        } else {
            throw new Exception('CreateProjectBigqueryMeta is required.');
        }

        $projectsClient = $this->clientManager->getProjectClient($credentials);

        /** @var array<string, string> $principal */
        $principal = (array) json_decode($credentials->getPrincipal());
        $formattedName = $projectsClient::projectName($principal['project_id']);
        $billingClient = $this->clientManager->getBillingClient($credentials);
        $billingInfo = $billingClient->getProjectBillingInfo($formattedName);
        $mainBillingAccount = $billingInfo->getBillingAccountName();

        $projectCreateResult = $this->createProject($projectsClient, $folderId, $projectId);
        $projectName = $projectCreateResult->getName();

        $serviceUsageClient = $this->clientManager->getServiceUsageClient($credentials);
        $this->enableServicesForProject($serviceUsageClient, $projectName);

        $billingInfo = new ProjectBillingInfo();
        $billingInfo->setBillingAccountName($mainBillingAccount);
        $billingInfo->setBillingEnabled(true);

        $billingClient->updateProjectBillingInfo($projectName, ['projectBillingInfo' => $billingInfo]);

        $projectServiceAccountId = $nameGenerator->createProjectServiceAccountId($command->getProjectId());
        $iamService = $this->clientManager->getIamClient($credentials);

        try {
            $projectServiceAccount = $iamService->createServiceAccount($projectServiceAccountId, $projectName);
        } catch (Throwable $e) {
            // project has been creates so it should be deleted
            $projectsClient->deleteProject($projectName);
            throw $e;
        }

        $storageManager = $this->clientManager->getStorageClient($credentials);
        $fileStorageBucket = $storageManager->bucket($fileStorageBucketName);
        $actualBucketPolicy = $fileStorageBucket->iam()->policy();

        // project service account can list and get files
        // project service account can export to bucket
        // project service account can delete files, needed also for export
        $actualBucketPolicy['bindings'][] = [
            'role' => 'roles/storage.objectAdmin',
            'members' => ['serviceAccount:' . $projectServiceAccount->getEmail()],
        ];
        $fileStorageBucket->iam()->setPolicy($actualBucketPolicy);

        $this->waitUntilServiceAccPropagate($iamService, $projectServiceAccount);

        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        $this->setPermissionsToServiceAccount($cloudResourceManager, $projectName, $projectServiceAccount->getEmail());

        [$privateKey, $publicPart] = $iamService->createKeyFileCredentials($projectServiceAccount);

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);
        $location = GCPClientManager::DEFAULT_LOCATION;
        $formattedParent = $analyticHubClient::locationName($projectCreateResult->getProjectId(), $location);

        $dataExchangeId = $nameGenerator->createDataExchangeId($command->getProjectId());
        $dataExchange = new DataExchange();
        $dataExchange->setDisplayName($dataExchangeId);
        $analyticHubClient->createDataExchange($formattedParent, $dataExchangeId, $dataExchange);

        return (new CreateProjectResponse())
            ->setProjectUserName($publicPart)
            ->setProjectPassword($privateKey)
            ->setProjectReadOnlyRoleName($dataExchangeId);
    }

    /**
     * @throws ApiException
     * @throws Exception
     * @throws ValidationException
     */
    private function createProject(ProjectsClient $projectsClient, string $folderId, string $projectId): Project
    {
        $project = new Project();

        $project->setParent('folders/' . $folderId);
        $project->setProjectId($projectId);
        $project->setDisplayName($projectId);

        $operationResponse = $projectsClient->createProject($project);
        $operationResponse->pollUntilComplete();
        if ($operationResponse->operationSucceeded()) {
            /** @var Project $projectCreateResult */
            $projectCreateResult = $operationResponse->getResult();
        } else {
            $error = $operationResponse->getError();
            assert($error !== null);
            throw new Exception($error->getMessage(), $error->getCode());
        }

        return $projectCreateResult;
    }

    /**
     * @throws ApiException
     * @throws Exception
     * @throws ValidationException
     */
    private function enableServicesForProject(ServiceUsageClient $serviceUsageClient, string $projectName): void
    {
        $operationResponse = $serviceUsageClient->batchEnableServices([
            'parent' => $projectName,
            'serviceIds' => self::ENABLED_SERVICES_FOR_PROJECT,
        ]);
        $operationResponse->pollUntilComplete();
        if (!$operationResponse->operationSucceeded()) {
            $error = $operationResponse->getError();
            assert($error !== null);
            throw new Exception($error->getMessage(), $error->getCode());
        }
    }

    private function setPermissionsToServiceAccount(
        Google_Service_CloudResourceManager $cloudResourceManagerClient,
        string $projectName,
        string $serviceAccEmail
    ): void {
        $getIamPolicyRequest = new GetIamPolicyRequest();
        $actualPolicy = $cloudResourceManagerClient->projects->getIamPolicy($projectName, $getIamPolicyRequest, []);

        $bigQueryDataOwnerBinding = new Binding();
        $bigQueryDataOwnerBinding->setMembers('serviceAccount:' . $serviceAccEmail);
        $bigQueryDataOwnerBinding->setRole(IAmPermissions::ROLES_BIGQUERY_DATA_OWNER);

        $serviceAccountCreatorBinding = new Binding();
        $serviceAccountCreatorBinding->setMembers('serviceAccount:' . $serviceAccEmail);
        $serviceAccountCreatorBinding->setRole(IAmPermissions::ROLES_IAM_SERVICE_ACCOUNT_CREATOR);

        $bigQueryJobUserBinding = new Binding();
        $bigQueryJobUserBinding->setMembers('serviceAccount:' . $serviceAccEmail);
        $bigQueryJobUserBinding->setRole(IAmPermissions::ROLES_BIGQUERY_JOB_USER);

        $ownerBinding = new Binding();
        $ownerBinding->setMembers('serviceAccount:' . $serviceAccEmail);
        $ownerBinding->setRole('roles/owner');

        $finalBinding[] = $actualPolicy->getBindings();
        $finalBinding[] = $bigQueryDataOwnerBinding;
        $finalBinding[] = $bigQueryJobUserBinding;
        $finalBinding[] = $ownerBinding;
        $finalBinding[] = $serviceAccountCreatorBinding;

        $policy = new Policy();
        $policy->setEtag($actualPolicy->getEtag());
        $policy->setVersion($actualPolicy->getVersion());
        $policy->setBindings($finalBinding);

        $setIamPolicyRequest = new SetIamPolicyRequest();
        $setIamPolicyRequest->setPolicy($policy);

        $cloudResourceManagerClient->projects->setIamPolicy($projectName, $setIamPolicyRequest);
    }

    private function waitUntilServiceAccPropagate(
        IAMServiceWrapper $iAmClient,
        ServiceAccount $projectServiceAccount
    ): void {
        $retryPolicy = new SimpleRetryPolicy(10);
        $backOffPolicy = new ExponentialRandomBackOffPolicy(
            1_000, // 1s
            1.8,
            10_000 // 1m
        );

        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $proxy->call(function () use ($iAmClient, $projectServiceAccount): void {
            $iAmClient->projects_serviceAccounts->get($projectServiceAccount->getName());
        });
    }
}
