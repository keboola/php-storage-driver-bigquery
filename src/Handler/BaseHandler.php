<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler;

use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

abstract class BaseHandler implements DriverCommandHandlerInterface
{
    protected LoggerInterface $logger;

    public function __construct(?LoggerInterface $logger = null)
    {
        if ($logger === null) {
            $this->logger = new NullLogger();
        } else {
            $this->logger = $logger;
        }
    }

    public function setLogger(LoggerInterface $logger): self
    {
        $this->logger = $logger;
        return $this;
    }
}
