<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
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
        Message $runtimeOptions,
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

        $credentialsMeta = CredentialsHelper::getBigQueryCredentialsMeta($credentials);

        /** @var array<string, string> $queryTags */
        $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

        $bqClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            $queryTags,
        );
        $projectCredentials = CredentialsHelper::getCredentialsArray($credentials);

        $nameGenerator = new NameGenerator($command->getStackPrefix());
        $newWsDatasetName = $nameGenerator->createWorkspaceObjectNameForWorkspaceId($command->getWorkspaceId());
        $newWsServiceAccId = $nameGenerator->createWorkspaceUserNameForWorkspaceId($command->getWorkspaceId());

        // create WS service acc
        $iamService = $this->clientManager->getIamClient($credentials);
        $projectName = 'projects/' . $projectCredentials['project_id'];

        $wsServiceAcc = Helper::createServiceAccount(
            $iamService,
            $newWsServiceAccId,
            $projectName,
            $this->internalLogger,
        );

        // create WS and grant WS service acc
        $dataset = $bqClient->createDataset($newWsDatasetName, [
            'access' => [
                'role' => IAmPermissions::ROLES_BIGQUERY_DATA_OWNER,
                'userByEmail' => $wsServiceAcc->getEmail(),
            ],
            'location' => $credentialsMeta->getRegion(),
            'retries' => 5,
        ]);

        // grant ROLES_BIGQUERY_JOB_USER to WS service acc
        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        Helper::grantProjectIamRoles(
            $cloudResourceManager,
            $projectName,
            $wsServiceAcc,
            $this->internalLogger,
        );

        // grant table-level IAM for direct grant tables
        foreach ($command->getDirectGrantTables() as $directGrantTable) {
            /** @var \Keboola\StorageDriver\Command\Workspace\DirectGrantTable $directGrantTable */
            $path = ProtobufHelper::repeatedStringToArray($directGrantTable->getPath());
            assert(count($path) > 0, 'DirectGrantTable path must not be empty');
            $datasetName = $path[0];

            $table = $bqClient->dataset($datasetName)
                ->table($directGrantTable->getTableName());

            $retryPolicy = new CallableRetryPolicy(function (Throwable $e) use ($datasetName, $directGrantTable) {
                $this->internalLogger->debug(sprintf(
                    'Try set table IAM policy for %s.%s Err: %s',
                    $datasetName,
                    $directGrantTable->getTableName(),
                    $e->getMessage(),
                ));
                return true;
            }, 5);
            $backOffPolicy = new ExponentialRandomBackOffPolicy(
                5_000, // 5s
                1.8,
                60_000, // 1m
            );
            $tableIamProxy = new RetryProxy($retryPolicy, $backOffPolicy);

            $tableIamProxy->call(function () use ($table, $wsServiceAcc, $datasetName, $directGrantTable): void {
                /** @var array<string, mixed> $policy */
                $policy = $table->iam()->policy();
                $role = IAmPermissions::ROLES_BIGQUERY_DATA_EDITOR;
                $member = 'serviceAccount:' . $wsServiceAcc->getEmail();
                /** @var array<int, array{role: string, members: string[]}> $bindings */
                $bindings = $policy['bindings'] ?? [];
                $found = false;
                foreach ($bindings as &$binding) {
                    if (($binding['role'] ?? '') === $role) {
                        if (!in_array($member, $binding['members'] ?? [], true)) {
                            $binding['members'][] = $member;
                        }
                        $found = true;
                        break;
                    }
                }
                unset($binding);
                if (!$found) {
                    $bindings[] = [
                        'role' => $role,
                        'members' => [$member],
                    ];
                }
                $policy['bindings'] = $bindings;
                $table->iam()->setPolicy($policy);
                $this->internalLogger->log(
                    LogLevel::DEBUG,
                    sprintf(
                        'Set table IAM policy (dataEditor) for %s on %s.%s',
                        $wsServiceAcc->getEmail(),
                        $datasetName,
                        $directGrantTable->getTableName(),
                    ),
                );
            });
        }

        // generate credentials
        [$privateKey, $publicPart,] = $iamService->createKeyFileCredentials($wsServiceAcc);

        return (new CreateWorkspaceResponse())
            ->setWorkspaceUserName($publicPart)
            ->setWorkspacePassword($privateKey)
            ->setWorkspaceObjectName($dataset->id());
    }
}
