<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\Handler\Backend\Init\InitBackendHandler;
use Keboola\StorageDriver\BigQuery\Handler\Backend\Remove\RemoveBackendHandler;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\RemoveBackendCommand;
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
        }

        throw new CommandNotSupportedException(get_class($command));
    }
}
