<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\ExecuteQuery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\ValueInterface;
use Google\Protobuf\Internal\Message;
use Google\Service\Iam\CreateServiceAccountKeyRequest;
use Google\Service\Iam\Resource\ProjectsServiceAccountsKeys;
use InvalidArgumentException;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\CreateWorkspaceHandler;
use Keboola\StorageDriver\Command\ExecuteQuery\ExecuteQueryCommand;
use Keboola\StorageDriver\Command\ExecuteQuery\ExecuteQueryResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use LogicException;
use Throwable;

final class ExecuteQueryHandler extends BaseHandler
{
    private const DEFAULT_QUERY_TIMEOUT_SECONDS = 60 * 60; // 1 hour in seconds

    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof ExecuteQueryCommand);

        assert($runtimeOptions->getMeta() === null);

        $datasetName = ProtobufHelper::repeatedStringToArray($command->getPathRestriction())[0] ?? null;

        // resolve query timeout
        $queryTimeout = self::DEFAULT_QUERY_TIMEOUT_SECONDS;
        if (is_int($command->getTimeout()) && $command->getTimeout() > 0) {
            $queryTimeout = $command->getTimeout();
        }

        if ($datasetName === null) {
            throw new InvalidArgumentException('Dataset name is required for query execution.');
        }

        if ($command->getRestriction() === 'bigQueryServiceAccount') {
            // Use service account by creating new private key for one run
            $restriction = $command->getBigQueryServiceAccount();
            if ($restriction === null) {
                throw new LogicException('BigQueryServiceAccount must be set');
            }
            [$serviceAccKeysService, $privateKey, $publicPart, $keyName] = $this->createServiceAccountKey(
                $credentials,
                $restriction,
            );
            /** @var array<string, string> $queryTags */
            $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

            $bqClient = $this->clientManager->getBigQueryClient(
                $runtimeOptions->getRunId(),
                new GenericBackendCredentials([
                    'host' => $credentials->getHost(),
                    'principal' => $publicPart,
                    'secret' => $privateKey,
                    'port' => $credentials->getPort(),
                    'meta' => $credentials->getMeta(),
                ]),
                $queryTags,
            );
            try {
                return $this->executeQuery(
                    $bqClient,
                    $datasetName,
                    $command->getQuery(),
                    $queryTimeout,
                );
            } finally {
                // delete the service account key
                $serviceAccKeysService->delete($keyName);
            }
        }

        //  execute query directly by provided credentials
        /** @var array<string, string> $queryTags */
        $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

        $bqClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            $queryTags,
        );
        return $this->executeQuery(
            $bqClient,
            $datasetName,
            $command->getQuery(),
            $queryTimeout,
        );
    }

    private function executeQuery(
        BigQueryClient $client,
        string $datasetName,
        string $query,
        int $timeout,
    ): ExecuteQueryResponse {
        try {
            // prepare query job configuration
            $dataset = $client->dataset($datasetName);
            $queryJobConfiguration = $client->query(
                $query,
                [
                    'configuration' => [
                        'jobTimeoutMs' => $timeout * 1000,
                    ],
                ],
            )->defaultDataset($dataset);

            // execute the query
            $result = $client->runQuery($queryJobConfiguration);
        } catch (Throwable $e) {
            $this->internalLogger->error($e->getMessage());
            return new ExecuteQueryResponse([
                'status' => ExecuteQueryResponse\Status::Error,
                'message' => DecodeErrorMessage::getErrorMessage($e),
            ]);
        }

        // compose the response message
        $message = 'Query executed successfully.';
        if (isset($result->identity()['jobId'])) {
            $message = sprintf(
                'Query "%s" executed successfully.',
                $result->identity()['jobId'],
            );
            if (isset($result->identity()['projectId']) && isset($result->identity()['location'])) {
                $message .= sprintf(
                    ' Project: %s, Location: %s',
                    $result->identity()['projectId'],
                    $result->identity()['location'],
                );
            }
        }

        $response = new ExecuteQueryResponse([
            'status' => ExecuteQueryResponse\Status::Success,
            'message' => $message,
        ]);
        if (isset($result->info()['schema'])) {
            $columns = array_map(fn(array $f) => $f['name'], $result->info()['schema']['fields']);
            $rows = array_map(function ($r) {
                $data  = [];
                if (!is_iterable($r)) {
                    throw new LogicException('Result rows must be iterable');
                }
                foreach ($r as $key => $value) {
                    if ($value instanceof ValueInterface) {
                        $data[$key] = $value->__toString();
                    } else {
                        $data[$key] = $value;
                    }
                }
                return new ExecuteQueryResponse\Data\Row([
                    'fields' => $data,
                ]);
            }, iterator_to_array($result->rows()));
            $response->setData(new ExecuteQueryResponse\Data([
                'rows' => $rows,
                'columns' => $columns,
            ]));
        }

        return $response;
    }

    /**
     * @return array{0:ProjectsServiceAccountsKeys, 1:string, 2:string, 3:string}
     */
    private function createServiceAccountKey(
        GenericBackendCredentials $credentials,
        ExecuteQueryCommand\BigQueryServiceAccount $restriction,
    ): array {
        // crate service account key
        $iamService = $this->clientManager->getIamClient($credentials);
        $serviceAccKeysService = $iamService->projects_serviceAccounts_keys;

        $serviceAccResourceName = sprintf(
            'projects/%s/serviceAccounts/%s',
            $restriction->getProjectId(),
            $restriction->getServiceAccountEmail(),
        );

        // create new service account key
        // This is needed as query must be restricted under workspace user, but we do not have credentials for it.
        // Impersonation would require high organization role in GCP which would be not granted in customers accounts.
        // as a workaround we create a new service account key for the workspace user and use it to execute the query
        $createServiceAccountKeyRequest = new CreateServiceAccountKeyRequest();
        $createServiceAccountKeyRequest->setPrivateKeyType(CreateWorkspaceHandler::PRIVATE_KEY_TYPE);
        $serviceAccount = $iamService->projects_serviceAccounts->get($serviceAccResourceName);
        [$privateKey, $publicPart, $keyName] = $iamService->createKeyFileCredentials($serviceAccount);

        return [$serviceAccKeysService, $privateKey, $publicPart, $keyName];
    }
}
