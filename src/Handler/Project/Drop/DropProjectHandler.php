<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Project\Drop;

use Exception;
use Google\Cloud\Billing\V1\ProjectBillingInfo;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;

final class DropProjectHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropProjectCommand);

        assert($runtimeOptions->getMeta() === null);

        $iamService = $this->clientManager->getIamClient($credentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;

        $credentialsMeta = CredentialsHelper::getBigQueryCredentialsMeta($credentials);
        $region = $credentialsMeta->getRegion();

        $commandMeta = $command->getMeta();
        if ($commandMeta === null) {
            throw new Exception('DropProjectBigqueryMeta is required.');
        }

        $commandMeta = $commandMeta->unpack();
        assert($commandMeta instanceof DropProjectCommand\DropProjectBigqueryMeta);
        $fileStorageBucketName = $commandMeta->getGcsFileBucketName();

        /** @var array<string, string>|false $publicPartKeyFile */
        $publicPartKeyFile = json_decode($command->getProjectUserName(), true, 512, JSON_THROW_ON_ERROR);
        assert($publicPartKeyFile !== false);

        $storageManager = $this->clientManager->getStorageClient($credentials);
        $fileStorageBucket = $storageManager->bucket($fileStorageBucketName);

        /** @var array{
         * kind: string,
         * resourceId: string,
         * version: int,
         * etag: string,
         * bindings: array<int, array{
         *  role: string,
         *  members: array<int, string>
         * }>
         * } $policy */
        $policy = $fileStorageBucket->iam()->policy();

        $policy = PolicyFilter::removeServiceAccFromBucketPolicy($policy, $publicPartKeyFile['client_email']);

        $fileStorageBucket->iam()->setPolicy($policy);

        $projectId = (string) $publicPartKeyFile['project_id'];
        $serviceAccountsInProject = $serviceAccountsService->listProjectsServiceAccounts(
            sprintf('projects/%s', $projectId),
        );
        foreach ($serviceAccountsInProject as $item) {
            $serviceAccountsService->delete(sprintf('projects/%s/serviceAccounts/%s', $projectId, $item->getEmail()));
        }
        $projectsClient = $this->clientManager->getProjectClient($credentials);

        $formattedName = $projectsClient::projectName($projectId);
        $billingClient = $this->clientManager->getBillingClient($credentials);
        $billingInfo = new ProjectBillingInfo();
        $billingInfo->setBillingEnabled(false);

        $billingClient->updateProjectBillingInfo($formattedName, ['projectBillingInfo' => $billingInfo]);

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);

        $dataExchangeId = $command->getReadOnlyRoleName();
        $formattedName = $analyticHubClient::dataExchangeName($projectId, $region, $dataExchangeId);
        $analyticHubClient->deleteDataExchange($formattedName);

        $formattedName = $projectsClient::projectName($projectId);
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
