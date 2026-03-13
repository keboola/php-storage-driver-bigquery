<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Create;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Table\CreateViewCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
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
        foreach ($columns as $col) {
            assert($col !== '', 'CreateViewCommand.columns must not contain empty strings');
        }
        if (count($columns) === 0) {
            $selectExpression = '*';
        } else {
            $selectExpression = implode(', ', array_map(
                static fn(string $col): string => BigqueryQuote::quoteSingleIdentifier($col),
                $columns,
            ));
        }

        // Build WHERE clause from filters
        $whereClauses = [];
        /** @var TableWhereFilter $filter */
        foreach ($command->getWhereFilters() as $filter) {
            $column = BigqueryQuote::quoteSingleIdentifier($filter->getColumnsName());
            /** @var string[] $values */
            $values = iterator_to_array($filter->getValues());
            $operator = $filter->getOperator();

            if (count($values) === 1) {
                $sqlOperator = match ($operator) {
                    Operator::eq => '=',
                    Operator::ne => '<>',
                    Operator::gt => '>',
                    Operator::ge => '>=',
                    Operator::lt => '<',
                    Operator::le => '<=',
                    default => throw new \LogicException(sprintf('Unsupported operator: %d', $operator)),
                };
                $whereClauses[] = sprintf('%s %s %s', $column, $sqlOperator, BigqueryQuote::quote($values[0]));
            } else {
                // Multi-value: only eq (IN) and ne (NOT IN) are supported
                $sqlOperator = match ($operator) {
                    Operator::eq => 'IN',
                    Operator::ne => 'NOT IN',
                    default => throw new \LogicException(
                        sprintf('Operator %d does not support multiple values', $operator),
                    ),
                };
                $quotedValues = implode(', ', array_map(
                    static fn(string $v): string => BigqueryQuote::quote($v),
                    $values,
                ));
                $whereClauses[] = sprintf('%s %s (%s)', $column, $sqlOperator, $quotedValues);
            }
        }

        $whereClause = count($whereClauses) > 0 ? ' WHERE ' . implode(' AND ', $whereClauses) : '';

        $sql = sprintf(
            'CREATE OR REPLACE VIEW %s.%s AS (SELECT %s FROM %s.%s%s)',
            BigqueryQuote::quoteSingleIdentifier($datasetName),
            BigqueryQuote::quoteSingleIdentifier($command->getViewName()),
            $selectExpression,
            BigqueryQuote::quoteSingleIdentifier($sourceDatasetName),
            BigqueryQuote::quoteSingleIdentifier($command->getSourceTableName()),
            $whereClause,
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
