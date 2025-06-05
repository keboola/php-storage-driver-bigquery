<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\ExecuteQuery;

use Google\Protobuf\Internal\Message;
use Google\Service\Iam\CreateServiceAccountKeyRequest;
use InvalidArgumentException;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\DecodeErrorMessage;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\CreateWorkspaceHandler;
use Keboola\StorageDriver\Command\ExecuteQuery\ExecuteQueryCommand;
use Keboola\StorageDriver\Command\ExecuteQuery\ExecuteQueryResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
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

        $restriction = $command->getRestriction();
        $restriction = match ($command->getRestriction()) {
            'bigQueryServiceAccount' => $command->getBigQueryServiceAccount(),
            default => throw new InvalidArgumentException(
                sprintf(
                // @phpcs:ignore Generic.Files.LineLength.MaxExceeded
                    'Unsupported restriction type: "%s". Currently supported is only "bigQueryServiceAccount" for workspace query execution.',
                    $restriction,
                ),
            ),
        };

        if ($restriction === null) {
            throw new InvalidArgumentException('Restriction is required for query execution.');
        }

        if ($datasetName === null) {
            throw new InvalidArgumentException('Dataset name is required for query execution.');
        }
        // resolve query timeout
        $queryTimeout = self::DEFAULT_QUERY_TIMEOUT_SECONDS;
        if (is_int($command->getTimeout()) && $command->getTimeout() > 0) {
            $queryTimeout = $command->getTimeout();
        }

        // crate service account key
        $iamService = $this->clientManager->getIamClient($credentials);
        $serviceAccKeysService = $iamService->projects_serviceAccounts_keys;

        $serviceAccResourceName = sprintf(
            'projects/%s/serviceAccounts/%s',
            $restriction->getProjectId(),
            $restriction->getServiceAccountEmail(),
        );

        // create new service account key
        $createServiceAccountKeyRequest = new CreateServiceAccountKeyRequest();
        $createServiceAccountKeyRequest->setPrivateKeyType(CreateWorkspaceHandler::PRIVATE_KEY_TYPE);
        $serviceAccount = $iamService->projects_serviceAccounts->get($serviceAccResourceName);
        [$privateKey, $publicPart, $keyName] = $iamService->createKeyFileCredentials($serviceAccount);

        try {
            // initialize BigQuery client with the service account key
            $bqClient = $this->clientManager->getBigQueryClient(
                $runtimeOptions->getRunId(),
                new GenericBackendCredentials([
                    'host' => $credentials->getHost(),
                    'principal' => $publicPart,
                    'secret' => $privateKey,
                    'port' => $credentials->getPort(),
                    'meta' => $credentials->getMeta(),
                ]),
            );
            // prepare query job configuration
            $dataset = $bqClient->dataset($datasetName);
            $queryJobConfiguration = $bqClient->query(
                $command->getQuery(),
                [
                    'configuration' => [
                        'jobTimeoutMs' => $queryTimeout * 1000,
                    ],
                ],
            )->defaultDataset($dataset);

            // execute the query
            $result = $bqClient->runQuery($queryJobConfiguration);
        } catch (Throwable $e) {
            $this->internalLogger->error($e->getMessage());
            return new ExecuteQueryResponse([
                'status' => ExecuteQueryResponse\Status::Error,
                'message' => DecodeErrorMessage::getErrorMessage($e),
            ]);
        } finally {
            // delete the service account key
            $serviceAccKeysService->delete($keyName);
        }

        $rows = array_map(fn($r) => new ExecuteQueryResponse\Data\Row([
            'fields' => $r,
        ]), iterator_to_array($result));

        return new ExecuteQueryResponse([
            'status' => ExecuteQueryResponse\Status::Success,
            'data' => new ExecuteQueryResponse\Data([
                'rows' => $rows,
                'columns' => array_map(fn(array $f) => $f['name'], $result->info()['schema']['fields']),
            ]),
        ]);
    }
}
