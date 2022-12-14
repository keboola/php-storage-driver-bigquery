<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Drop;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Throwable;

class DropTableHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
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
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropTableCommand);

        // validate
        assert($command->getPath()->count() === 1, 'DropTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'DropTableCommand.tableName is required');

        $bqClient = $this->clientManager->getBigQueryClient($credentials);
        /** @var string $datasetName */
        $datasetName = $command->getPath()[0]; // bucket name
        $dateset = $bqClient->dataset($datasetName);
        $table = $dateset->table($command->getTableName());

        // drop table
        try {
            $table->delete();
        } catch (Throwable $e) {
            if (!$command->getIgnoreErrors()) {
                throw $e;
            }
        }

        return null;
    }
}
