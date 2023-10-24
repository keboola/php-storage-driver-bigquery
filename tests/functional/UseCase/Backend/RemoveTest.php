<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Backend;

use Keboola\StorageDriver\BigQuery\Handler\Backend\Remove\RemoveBackendHandler;
use Keboola\StorageDriver\Command\Backend\RemoveBackendCommand;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\FunctionalTests\BaseCase;

class RemoveTest extends BaseCase
{
    /**
     * @doesNotPerformAssertions
     */
    public function testInitBackend(): void
    {
        $handler = new RemoveBackendHandler();
        $handler->setLogger($this->log);
        $command = new RemoveBackendCommand();
        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );
    }
}
