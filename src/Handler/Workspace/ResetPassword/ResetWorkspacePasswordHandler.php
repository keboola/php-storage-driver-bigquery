<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\ResetPassword;

use Exception;
use Google\Protobuf\Internal\Message;
use Google\Service\Iam\ServiceAccountKey;
use Google_Service_Iam_CreateServiceAccountKeyRequest;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\CreateWorkspaceHandler;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordCommand;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;

final class ResetWorkspacePasswordHandler extends BaseHandler
{
    public const KEY_TYPE_USER_MANAGED = 'USER_MANAGED';
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param ResetWorkspacePasswordCommand $command
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof ResetWorkspacePasswordCommand);

        assert($runtimeOptions->getMeta() === null);

        // validate
        assert($command->getWorkspaceUserName() !== '', 'ResetWorkspacePasswordCommand.workspaceUserName is required');

        /** @var array<string, string> $keyData */
        $keyData = (array) json_decode($command->getWorkspaceUserName(), true, 512, JSON_THROW_ON_ERROR);
        $projectId = $keyData['project_id'];
        $wsServiceAccEmail = $keyData['client_email'];

        $iamService = $this->clientManager->getIamClient($credentials);
        $serviceAccKeysService = $iamService->projects_serviceAccounts_keys;

        $serviceAccResourceName = sprintf('projects/%s/serviceAccounts/%s', $projectId, $wsServiceAccEmail);
        $keys = $serviceAccKeysService->listProjectsServiceAccountsKeys(
            $serviceAccResourceName,
            ['keyTypes' => self::KEY_TYPE_USER_MANAGED],
        );

        /** @var ServiceAccountKey $key */
        foreach ($keys as $key) {
            $serviceAccKeysService->delete($key->getName());
        }

        $createServiceAccountKeyRequest = new Google_Service_Iam_CreateServiceAccountKeyRequest();
        $createServiceAccountKeyRequest->setPrivateKeyType(CreateWorkspaceHandler::PRIVATE_KEY_TYPE);
        $key = $serviceAccKeysService->create($serviceAccResourceName, $createServiceAccountKeyRequest);
        $json = base64_decode($key->getPrivateKeyData());
        $keyData = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        if (!is_array($keyData)) {
            throw new Exception('Project key credentials missing.');
        }

        // separate private and public part
        $privateKey = $keyData[CreateWorkspaceHandler::KEY_DATA_PROPERTY_PRIVATE_KEY];
        unset($keyData[CreateWorkspaceHandler::KEY_DATA_PROPERTY_PRIVATE_KEY]);
        $publicPart = json_encode($keyData);
        assert($publicPart !== false);

        return (new ResetWorkspacePasswordResponse())
            ->setWorkspaceUserName($publicPart)
            ->setWorkspacePassword($privateKey);
    }
}
