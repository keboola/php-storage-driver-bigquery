<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Create;

use Google\Protobuf\Internal\Message;
use Google\Service\CloudResourceManager\Binding;
use Google\Service\CloudResourceManager\GetIamPolicyRequest;
use Google\Service\CloudResourceManager\Policy;
use Google\Service\CloudResourceManager\SetIamPolicyRequest;
use Google_Service_CloudResourceManager;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use RuntimeException;

final class CreateWorkspaceHandler implements DriverCommandHandlerInterface
{
    private const IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES = [
        IAmPermissions::ROLES_BIGQUERY_DATA_VIEWER, // readOnly access
        IAmPermissions::ROLES_BIGQUERY_JOB_USER,
    ];
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
        $wsServiceAcc = $iamService->createServiceAccount($newWsServiceAccId, $projectName);

        // create WS and grant WS service acc
        $dataset = $bqClient->createDataset($newWsDatasetName, [
            'access' => [
                'role' => IAmPermissions::ROLES_BIGQUERY_DATA_OWNER,
                'userByEmail' => $wsServiceAcc->getEmail(),
            ],
        ]);

        // grant ROLES_BIGQUERY_JOB_USER to WS service acc
        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        $getIamPolicyRequest = new GetIamPolicyRequest();
        $actualPolicy = $cloudResourceManager->projects->getIamPolicy($projectName, $getIamPolicyRequest, []);
        $finalBinding[] = $actualPolicy->getBindings();

        foreach (self::IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES as $role) {
            $bigQueryJobUserBinding = new Binding();
            $bigQueryJobUserBinding->setMembers('serviceAccount:' . $wsServiceAcc->getEmail());
            $bigQueryJobUserBinding->setRole($role);
            $finalBinding[] = $bigQueryJobUserBinding;
        }

        $policy = new Policy();
        $policy->setBindings($finalBinding);
        $setIamPolicyRequest = new SetIamPolicyRequest();
        $setIamPolicyRequest->setPolicy($policy);

        $retryPolicy = new SimpleRetryPolicy(10);
        $backOffPolicy = new ExponentialBackOffPolicy();
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $proxy->call(function () use ($cloudResourceManager, $projectName, $setIamPolicyRequest, $wsServiceAcc): void {
            $cloudResourceManager->projects->setIamPolicy($projectName, $setIamPolicyRequest);
            $this->waitUntilBindingsPropagate($cloudResourceManager, $projectName, $wsServiceAcc->getEmail());
        });

        // generate credentials
        [$privateKey, $publicPart] = $iamService->createKeyFileCredentials($wsServiceAcc);

        return (new CreateWorkspaceResponse())
            ->setWorkspaceUserName($publicPart)
            ->setWorkspacePassword($privateKey)
            ->setWorkspaceObjectName($dataset->id());
    }

    private function waitUntilBindingsPropagate(
        Google_Service_CloudResourceManager $cloudResourceManager,
        string $projectName,
        string $wsServiceAccEmail
    ): void {
        $retryPolicy = new SimpleRetryPolicy(10);
        $backOffPolicy = new ExponentialBackOffPolicy(
            initialInterval: 30_000, // 30s
            multiplier: 1.2, // 180s
            maxInterval: 180_000, // 30s
        );

        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $proxy->call(function () use ($cloudResourceManager, $projectName, $wsServiceAccEmail): void {
            $actualPolicy = $cloudResourceManager->projects->getIamPolicy($projectName, (new GetIamPolicyRequest()));
            $actualPolicy = $actualPolicy->getBindings();

            $serviceAccRoles = [];
            foreach ($actualPolicy as $policy) {
                if (in_array('serviceAccount:' . $wsServiceAccEmail, $policy->getMembers())) {
                    $serviceAccRoles[] = $policy->getRole();
                }
            }

            sort($serviceAccRoles);

            // ws service acc must have a job user role to be able to run queries
            if ($serviceAccRoles !== self::IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES) {
                throw new RuntimeException(sprintf(
                    'Workspace service account has incorrect roles. Expected roles: [%s], actual roles: [%s]',
                    implode(',', self::IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES),
                    implode(',', $serviceAccRoles)
                ));
            }
        });
    }
}
