<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Backend\Remove;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\Command\Backend\RemoveBackendCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;

final class RemoveBackendHandler extends BaseHandler
{
    /**
     * @inheritDoc
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof RemoveBackendCommand);
        assert($runtimeOptions->getMeta() === null);

        return null;
    }
}
