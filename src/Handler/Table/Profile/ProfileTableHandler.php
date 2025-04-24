<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Profile;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\Command\Table\CreateProfileTableCommand;
use Keboola\StorageDriver\Command\Table\CreateProfileTableResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class ProfileTableHandler extends BaseHandler
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
     * @param CreateProfileTableCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message|null {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateProfileTableCommand);

        assert($runtimeOptions->getMeta() === null);

        // Validate
        assert($command->getPath()->count() === 1, 'DropTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'DropTableCommand.tableName is required');

        return (new CreateProfileTableResponse())
            ->setPath($command->getPath())
            ->setTableName($command->getTableName());
    }
}
