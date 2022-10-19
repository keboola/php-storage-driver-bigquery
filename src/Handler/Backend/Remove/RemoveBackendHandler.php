<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Backend\Remove;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Backend\RemoveBackendCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class RemoveBackendHandler implements DriverCommandHandlerInterface
{
    /**
     * @inheritDoc
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof RemoveBackendCommand);

        return null;
    }
}
