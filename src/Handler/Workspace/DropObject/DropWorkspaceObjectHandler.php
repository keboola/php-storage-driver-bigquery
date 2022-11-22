<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\DropObject;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceObjectCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

class DropWorkspaceObjectHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param DropWorkspaceObjectCommand $command
     */
    public function __invoke(
        Message $credentials, // workspace credentials
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropWorkspaceObjectCommand);

        // validate
        assert(
            $command->getWorkspaceObjectName() !== '',
            'DropWorkspaceObjectCommand.workspaceObjectName is required'
        );
        assert(
            $command->getObjectNameToDrop() !== '',
            'DropWorkspaceObjectCommand.objectNameToDrop is required'
        );

        $bqClient = $this->clientManager->getBigQueryClient($credentials);

        $isTableExists = $this->isTableExists(
            $bqClient,
            $command->getWorkspaceObjectName(),
            $command->getObjectNameToDrop()
        );
        if ($command->getIgnoreIfNotExists() && !$isTableExists) {
            return null;
        }

        $bqClient->runQuery($bqClient->query(sprintf(
            'DROP TABLE %s.%s;',
            BigqueryQuote::quoteSingleIdentifier($command->getWorkspaceObjectName()),
            BigqueryQuote::quoteSingleIdentifier($command->getObjectNameToDrop())
        )));

        return null;
    }

    private function isTableExists(BigQueryClient $projectBqClient, string $datasetName, string $tableName): bool
    {
        $dataset = $projectBqClient->dataset($datasetName);
        $table = $dataset->table($tableName);
        return $table->exists();
    }
}
