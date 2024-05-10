<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Alter;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\Command\Table\DropPrimaryKeyCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class DropPrimaryKeyHandler extends BaseHandler
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
     * @param DropPrimaryKeyCommand $command
     * @param string[] $features
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropPrimaryKeyCommand);

        // validate
        assert($command->getPath()->count() === 1, 'DropPrimaryKeyCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'DropPrimaryKeyCommand.tableName is required');

        $bqClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);

        /** @var string $databaseName */
        $databaseName = $command->getPath()[0];

        $bqClient->dataset($databaseName)->table($command->getTableName())->update(
            [
                'tableConstraints' => [
                    'primaryKey' => [
                        'columns' => [],
                    ],
                ],
            ],
        );
        return null;
    }
}
