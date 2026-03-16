<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\QueryBuilder\CreateViewQueryBuilder;
use Keboola\StorageDriver\Command\Table\CreateViewCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

final class CreateViewHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateViewCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateViewCommand);

        assert($command->getPath()->count() === 1, 'CreateViewCommand.path is required and size must equal 1');
        assert($command->getViewName() !== '', 'CreateViewCommand.viewName is required');
        assert($command->getSourceTableName() !== '', 'CreateViewCommand.sourceTableName is required');

        /** @var array<string, string> $queryTags */
        $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

        $bqClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            $queryTags,
        );

        /** @var string $datasetName */
        $datasetName = $command->getPath()[0];

        // Source dataset: use sourcePath if provided, otherwise same as path
        if ($command->getSourcePath()->count() > 0) {
            assert(
                $command->getSourcePath()->count() === 1,
                'CreateViewCommand.sourcePath size must equal 1',
            );
            $sourceDatasetName = $command->getSourcePath()[0];
            assert(
                is_string($sourceDatasetName) && $sourceDatasetName !== '',
                'CreateViewCommand.sourcePath[0] must be non-empty string',
            );
        } else {
            $sourceDatasetName = $datasetName;
        }

        /** @var string[] $columns */
        $columns = iterator_to_array($command->getColumns());

        $queryBuilder = new CreateViewQueryBuilder();
        $sql = $queryBuilder->buildCreateViewSql(
            $datasetName,
            $command->getViewName(),
            $sourceDatasetName,
            $command->getSourceTableName(),
            $columns,
            $command->getWhereFilters(),
        );

        $bqClient->runQuery($bqClient->query($sql));

        if ($sourceDatasetName !== $datasetName) {
            // Cross-dataset VIEW: grant authorized view access on source dataset.
            // This allows linked dataset consumers to query the VIEW even though
            // they have no direct access to the source dataset.
            $credentialsArr = CredentialsHelper::getCredentialsArray($credentials);
            $projectId = $credentialsArr['project_id'];

            $sourceDataset = $bqClient->dataset($sourceDatasetName);

            $authorizedView = [
                'projectId' => $projectId,
                'datasetId' => $datasetName,
                'tableId' => $command->getViewName(),
            ];

            $retryPolicy = new CallableRetryPolicy(function (Throwable $e): bool {
                // Retry only on 412 Precondition Failed (etag mismatch)
                return $e->getCode() === 412;
            }, 5);
            $proxy = new RetryProxy($retryPolicy, new ExponentialRandomBackOffPolicy(500, 1.8, 5_000));

            $proxy->call(function () use ($sourceDataset, $authorizedView): void {
                $info = $sourceDataset->reload();
                /** @var list<array<string, mixed>> $currentAccess */
                $currentAccess = $info['access'] ?? [];

                // Idempotency check (for CREATE OR REPLACE VIEW re-runs)
                foreach ($currentAccess as $entry) {
                    if (isset($entry['view']) && $entry['view'] === $authorizedView) {
                        return;
                    }
                }

                $currentAccess[] = ['view' => $authorizedView];
                $sourceDataset->update(
                    ['access' => $currentAccess],
                    ['etag' => $info['etag'], 'retries' => 0],
                );
            });
        }

        return null;
    }
}
