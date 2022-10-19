<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\Handler\Backend\Init\InitBackendHandler;
use Keboola\StorageDriver\BigQuery\Handler\Backend\Remove\RemoveBackendHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Drop\DropProjectHandler;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\RemoveBackendCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Contract\Driver\ClientInterface;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\CommandNotSupportedException;

class BigQueryDriverClient implements ClientInterface
{
    public function runCommand(Message $credentials, Message $command, array $features): ?Message
    {
        assert($credentials instanceof GenericBackendCredentials);
        $manager = new GCPClientManager();
        $handler = $this->getHandler($command, $manager);

        return $handler(
            $credentials,
            $command,
            $features
        );
    }

    private function getHandler(Message $command, GCPClientManager $manager): DriverCommandHandlerInterface
    {
        switch (true) {
            case $command instanceof InitBackendCommand:
                return new InitBackendHandler($manager);
            case $command instanceof RemoveBackendCommand:
                return new RemoveBackendHandler();
            case $command instanceof CreateProjectCommand:
                return new CreateProjectHandler($manager);
            case $command instanceof DropProjectCommand:
                return new DropProjectHandler($manager);
        }

        throw new CommandNotSupportedException(get_class($command));
    }
}
