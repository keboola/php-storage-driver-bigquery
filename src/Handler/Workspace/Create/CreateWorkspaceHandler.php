<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Create;

use Exception;
use Google\Protobuf\Internal\Message;
use Google_Service_CloudResourceManager_Binding;
use Google_Service_CloudResourceManager_GetIamPolicyRequest;
use Google_Service_CloudResourceManager_Policy;
use Google_Service_CloudResourceManager_SetIamPolicyRequest;
use Google_Service_Iam_CreateServiceAccountKeyRequest;
use Google_Service_Iam_CreateServiceAccountRequest;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class CreateWorkspaceHandler implements DriverCommandHandlerInterface
{
    public const PRIVATE_KEY_TYPE = 'TYPE_GOOGLE_CREDENTIALS_FILE';
    public const KEY_DATA_PROPERTY_PRIVATE_KEY = 'private_key';

    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
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
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateWorkspaceCommand);

        // validate
        assert($command->getStackPrefix() !== '', 'CreateWorkspaceCommand.stackPrefix is required');
        assert($command->getWorkspaceId() !== '', 'CreateWorkspaceCommand.workspaceId is required');
        assert(
            $command->getProjectReadOnlyRoleName() !== '',
            'CreateWorkspaceCommand.projectReadOnlyRoleName is required',
        );

        $bqClient = $this->clientManager->getBigQueryClient($credentials);
        $projectCredentials = CredentialsHelper::getCredentialsArray($credentials);

        $nameGenerator = new NameGenerator($command->getStackPrefix());
        $newWsDatasetName = $nameGenerator->createWorkspaceObjectNameForWorkspaceId($command->getWorkspaceId());
        $newWsServiceAccName = $nameGenerator->createWorkspaceUserNameForWorkspaceId($command->getWorkspaceId());

        // create WS service acc
        $iamService = $this->clientManager->getIamClient($credentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $createServiceAccountRequest = new Google_Service_Iam_CreateServiceAccountRequest();
        $createServiceAccountRequest->setAccountId($newWsServiceAccName);
        $projectName = 'projects/' . $projectCredentials['project_id'];
        $wsServiceAcc = $serviceAccountsService->create($projectName, $createServiceAccountRequest);

        // create WS and grant WS service acc
        $dataset = $bqClient->createDataset($newWsDatasetName, [
            'access' => [
                'role' => IAmPermissions::ROLES_BIGQUERY_DATA_OWNER,
                'userByEmail' => $wsServiceAcc->getEmail(),
            ],
        ]);

        // grant ROLES_BIGQUERY_JOB_USER to WS service acc
        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        $getIamPolicyRequest = new Google_Service_CloudResourceManager_GetIamPolicyRequest();
        $actualPolicy = $cloudResourceManager->projects->getIamPolicy($projectName, $getIamPolicyRequest, []);
        $finalBinding[] = $actualPolicy->getBindings();

        $bigQueryJobUserBinding = new Google_Service_CloudResourceManager_Binding();
        $bigQueryJobUserBinding->setMembers('serviceAccount:' . $wsServiceAcc->getEmail());
        $bigQueryJobUserBinding->setRole(IAmPermissions::ROLES_BIGQUERY_JOB_USER);
        $finalBinding[] = $bigQueryJobUserBinding;

        // set read only access to the datasets in project
        $bigQueryDataViewerBinding = new Google_Service_CloudResourceManager_Binding();
        $bigQueryDataViewerBinding->setMembers('serviceAccount:' . $wsServiceAcc->getEmail());
        $bigQueryDataViewerBinding->setRole(IAmPermissions::ROLES_BIGQUERY_DATA_VIEWER);
        $finalBinding[] = $bigQueryDataViewerBinding;

        $policy = new Google_Service_CloudResourceManager_Policy();
        $policy->setBindings($finalBinding);
        $setIamPolicyRequest = new Google_Service_CloudResourceManager_SetIamPolicyRequest();
        $setIamPolicyRequest->setPolicy($policy);
        $cloudResourceManager->projects->setIamPolicy($projectName, $setIamPolicyRequest);

        // generate credentials
        $serviceAccKeysService = $iamService->projects_serviceAccounts_keys;
        $createServiceAccountKeyRequest = new Google_Service_Iam_CreateServiceAccountKeyRequest();
        $createServiceAccountKeyRequest->setPrivateKeyType(self::PRIVATE_KEY_TYPE);
        $key = $serviceAccKeysService->create($wsServiceAcc->getName(), $createServiceAccountKeyRequest);
        $json = base64_decode($key->getPrivateKeyData());
        $keyData = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        if (!is_array($keyData)) {
            throw new Exception('Project key credentials missing.');
        }

        // separate private and public part
        $privateKey = $keyData[self::KEY_DATA_PROPERTY_PRIVATE_KEY];
        unset($keyData[self::KEY_DATA_PROPERTY_PRIVATE_KEY]);
        $publicPart = json_encode($keyData);
        assert($publicPart !== false);

        return (new CreateWorkspaceResponse())
            ->setWorkspaceUserName($publicPart)
            ->setWorkspacePassword($privateKey)
            ->setWorkspaceObjectName($dataset->id());
    }
}
