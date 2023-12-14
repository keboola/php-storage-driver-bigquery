<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Helpers;

use Google\ApiCore\GPBType;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\StringValue;
use Keboola\StorageDriver\Command\Common\LogMessage;
use Psr\Log\AbstractLogger;
use Psr\Log\LogLevel;
use Stringable;

class UserInMemoryLogger extends AbstractLogger
{
    private const LOG_LEVEL_MAP = [
        LogLevel::EMERGENCY => LogMessage\Level::Emergency,
        LogLevel::ALERT => LogMessage\Level::Alert,
        LogLevel::CRITICAL => LogMessage\Level::Critical,
        LogLevel::ERROR => LogMessage\Level::Error,
        LogLevel::WARNING => LogMessage\Level::Warning,
        LogLevel::NOTICE => LogMessage\Level::Notice,
        LogLevel::INFO => LogMessage\Level::Informational,
        LogLevel::DEBUG => LogMessage\Level::Debug,
    ];

    /**
     * @var RepeatedField<LogMessage>
     */
    private RepeatedField $logs;

    public function __construct()
    {
        $this->logs = new RepeatedField(GPBType::MESSAGE, LogMessage::class);
    }

    /**
     * @throws \JsonException
     * @param mixed $level
     * @param Stringable|string $message
     * @param array<mixed> $context
     */
    // phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingAnyTypeHint,SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function log(
        $level,
        $message,
        array $context = [],
    ): void {
        $msg = new LogMessage();
        $msg->setLevel(self::LOG_LEVEL_MAP[$level]);
        $msg->setMessage((string) $message);
        $ctx = new Any();
        $ctx->pack(
            (new StringValue())
                ->setValue(json_encode($context, JSON_THROW_ON_ERROR)),
        );
        $msg->setContext($ctx);
        $this->logs[] = $msg;
    }

    /**
     * @return RepeatedField<LogMessage>
     */
    public function getLogs(): RepeatedField
    {
        return $this->logs;
    }
}
