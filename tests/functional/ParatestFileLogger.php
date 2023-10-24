<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use DateTime;
use Psr\Log\AbstractLogger;
use Stringable;
use Symfony\Component\Filesystem\Filesystem;

/**
 * Logging output to file
 * This is needed to get output from paratest mostly for debugging purposes
 */
class ParatestFileLogger extends AbstractLogger
{
    private readonly Filesystem $fs;

    private string $prefix = '';

    public function __construct(
        private readonly string $name
    ) {
        $this->fs = new Filesystem();
    }

    private function getLogFilename(): string
    {
        return __DIR__ . '/../../logs/' . $this->name . '.log';
    }

    public function setPrefix(string $prefix): void
    {
        $this->prefix = $prefix . ':: ';
    }

    public function add(string $message): void
    {
        $now = new DateTime('now');
        $now = $now->format('m-d-Y H:i:s.u');
        $this->fs->appendToFile(
            filename: $this->getLogFilename(),
            content: $now . ' ' . $this->prefix . $message . "\n"
        );
    }

    /**
     * @param array<mixed> $context
     */
    public function log(mixed $level, Stringable|string $message, array $context = []): void
    {
        $this->add('[' . $level . '] ' . $message . ' ' . json_encode($context, JSON_THROW_ON_ERROR, 512));
    }
}
