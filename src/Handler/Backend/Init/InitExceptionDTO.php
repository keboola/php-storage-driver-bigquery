<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Backend\Init;

final class InitExceptionDTO
{
    public function __construct(
        private readonly string $message,
        private readonly string|null $reason,
    ) {
    }

    public function getMessage(): string
    {
        $message = $this->message;
        if (is_string($this->reason) && $this->reason !== '') {
            $message .= sprintf(' Reason: %s', $this->reason);
        }
        return $message;
    }
}
