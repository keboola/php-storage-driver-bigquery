<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Clear;

use Google\Cloud\BigQuery\Table;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Workspace\ClearWorkspaceCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Throwable;

final class ClearWorkspaceHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param ClearWorkspaceCommand $command
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof ClearWorkspaceCommand);

        // validate
        assert($command->getWorkspaceObjectName() !== '', 'ClearWorkspaceCommand.workspaceObjectName is required');

        $bqClient = $this->clientManager->getBigQueryClient($credentials);

        $tablesInDataset = $bqClient->dataset($command->getWorkspaceObjectName())->tables();
        try {
            /*
             * Bigquery doesn't support something like DELETE ALL FROM SCHEMA
             * in case if dataset not exist fetch and in case of error
             * throw exception or return null in case of ignore error
            */
            $tablesInDataset->current();
        } catch (Throwable $e) {
            if (!$command->getIgnoreErrors()) {
                throw $e;
            }
            return null;
        }

        /** @var Table $table */
        foreach ($tablesInDataset as $table) {
            try {
                $table->delete();
            } catch (Throwable $e) {
                if (!$command->getIgnoreErrors()) {
                    throw $e;
                }
            }
        }

        return null;
    }
}
