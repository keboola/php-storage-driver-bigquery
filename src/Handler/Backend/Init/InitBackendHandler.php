<?php

namespace Keboola\StorageDriver\BigQuery\Handler\Backend\Init;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;

final class InitBackendHandler implements DriverCommandHandlerInterface
{
    public function __invoke(Message $credentials, Message $command, array $features): ?Message
    {
        // TODO: Implement __invoke() method.
    }
}
