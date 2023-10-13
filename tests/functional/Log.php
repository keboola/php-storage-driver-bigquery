<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use Symfony\Component\Filesystem\Filesystem;

/**
 * Logging output to file
 * This is needed to get output from paratest mostly for debugging purposes
 */
class Log
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
        $this->fs->appendToFile(
            filename: $this->getLogFilename(),
            content: $this->prefix . $message . "\n"
        );
    }
}
