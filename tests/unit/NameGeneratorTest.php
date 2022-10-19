<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Shared\NameGenerator;

use Keboola\StorageDriver\BigQuery\NameGenerator;
use PHPUnit\Framework\TestCase;

class NameGeneratorTest extends TestCase
{
    public function testProjectIdGenerator(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');
        $this->assertSame('kbc-prefix-project-1', $nameGenerator->createProjectId('project-1'));
    }

    public function testProjectServiceAccountIddGenerator(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');
        $this->assertSame('kbc-prefix-acc-1', $nameGenerator->createProjectServiceAccountId('acc-1'));
    }

    public function testObjectNameForBucketInProjectGenerator(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');

        $this->assertSame('in_c_bucket', $nameGenerator->createObjectNameForBucketInProject('in.c-bucket'));
    }
}
