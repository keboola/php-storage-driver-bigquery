<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Shared\NameGenerator;

use Keboola\StorageDriver\BigQuery\NameGenerator;
use PHPUnit\Framework\TestCase;

class NameGeneratorTest extends TestCase
{
    public function testProjectIdGenerator(): void
    {
        $nameGenerator = new NameGenerator('prefix');
        $this->assertSame('prefix-project-1', $nameGenerator->createProjectId('project-1'));
    }

    public function testProjectServiceAccountIddGenerator(): void
    {
        $nameGenerator = new NameGenerator('prefix');
        $this->assertSame('prefix-acc-1', $nameGenerator->createProjectServiceAccountId('acc-1'));
    }
}
