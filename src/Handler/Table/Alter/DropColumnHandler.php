<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Alter;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Table\DropColumnCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;

final class DropColumnHandler extends BaseHandler
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
     * @param DropColumnCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropColumnCommand);

        assert($runtimeOptions->getMeta() === null);

        assert($command->getPath()->count() === 1, 'DropColumnCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'DropColumnCommand.tableName is required');

        /** @var array<string, string> $queryTags */
        $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

        $bqClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            $queryTags,
        );

        // define columns
        // validate
        assert($command->getColumnName() !== '', 'DropColumnCommand.columnName is required');

        // build sql
        $builder = new BigqueryTableQueryBuilder();
        /** @var string $databaseName */
        $databaseName = $command->getPath()[0];
        $dropColumnSql = $builder->getDropColumnCommand(
            $databaseName,
            $command->getTableName(),
            $command->getColumnName(),
        );

        $bqClient->runQuery($bqClient->query($dropColumnSql));

        return null;
    }
}
