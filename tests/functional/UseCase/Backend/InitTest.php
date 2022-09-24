<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Backend;

use Keboola\StorageDriver\BigQuery\Handler\Backend\Init\InitBackendHandler;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\InitBackendResponse;
use PHPUnit\Framework\TestCase;

class InitTest extends TestCase
{
    public function testInitBackend(): void
    {
        $handler = new InitBackendHandler($this->sessionManager);
        $command = new InitBackendCommand();
        $response = $handler(
            $this->getCredentials(),
            $command,
            []
        );
        $this->assertInstanceOf(InitBackendResponse::class, $response);
    }

    private function getCredentials()
    {

    }
}
