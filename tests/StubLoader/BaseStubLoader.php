<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\TestsStubLoader;

use Symfony\Component\Finder\Finder;

abstract class BaseStubLoader
{
    public const BASE_DIR = __DIR__ . '/../data/';

    abstract public function load(): void;
}
