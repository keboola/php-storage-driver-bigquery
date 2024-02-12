<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Throwable;

final class DropTableHandler extends BaseHandler
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
     * @param DropTableCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropTableCommand);

        assert($runtimeOptions->getMeta() === null);

        // validate
        assert($command->getPath()->count() === 1, 'DropTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'DropTableCommand.tableName is required');

        $bqClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);
        /** @var string $datasetName */
        $datasetName = $command->getPath()[0]; // bucket name
        $dateset = $bqClient->dataset($datasetName);
        $table = $dateset->table($command->getTableName());
        $table->delete();

        return null;
    }
}
