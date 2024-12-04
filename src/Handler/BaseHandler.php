<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler;

use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\Handler\Helpers\UserInMemoryLogger;
use Keboola\StorageDriver\Command\Common\LogMessage;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

abstract class BaseHandler implements DriverCommandHandlerInterface
{
    protected const DEFAULT_RETRY_OVERRIDE = 2;
    protected LoggerInterface $internalLogger;

    protected UserInMemoryLogger $userLogger;

    public function __construct(
        ?LoggerInterface $internalLogger = null,
    ) {
        if ($internalLogger === null) {
            $this->internalLogger = new NullLogger();
        } else {
            $this->internalLogger = $internalLogger;
        }
        $this->userLogger = new UserInMemoryLogger();
    }

    public function setInternalLogger(LoggerInterface $logger): self
    {
        $this->internalLogger = $logger;
        return $this;
    }

    /**
     * @return RepeatedField<LogMessage>
     */
    public function getMessages(): RepeatedField
    {
        return $this->userLogger->getLogs();
    }
}
