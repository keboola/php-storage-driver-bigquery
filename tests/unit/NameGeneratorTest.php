<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests;

use Keboola\StorageDriver\BigQuery\NameGenerator;
use PHPUnit\Framework\TestCase;

class NameGeneratorTest extends TestCase
{
    public function testProjectIdGenerator(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');
        $projectId = $nameGenerator->createProjectId('project-1');
        $this->assertStringContainsString('kbc-prefix-project-1', $projectId);
        $this->assertEquals(25, strlen($projectId));
    }

    public function testProjectServiceAccountIddGenerator(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');
        $this->assertSame('kbc-prefix-acc-1', $nameGenerator->createProjectServiceAccountId('acc-1'));
    }

    public function testObjectNameForBucketInProjectGenerator(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');

        $this->assertSame('in_c_bucket', $nameGenerator->createObjectNameForBucketInProject('in.c-bucket', null));
    }

    public function testObjectNameForBucketInProjectWithEmptyStringBranchIdGenerator(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');

        $this->assertSame('in_c_bucket', $nameGenerator->createObjectNameForBucketInProject('in.c-bucket', ''));
    }

    public function testObjectNameForBucketInProjectWithBranchIdGenerator(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');

        $this->assertSame('123_in_c_bucket', $nameGenerator->createObjectNameForBucketInProject('in.c-bucket', '123'));
    }

    public function testCreateWorkspaceObjectNameForWorkspaceId(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');

        $this->assertSame('WORKSPACE_123', $nameGenerator->createWorkspaceObjectNameForWorkspaceId('123'));
    }

    public function testCreateWorkspaceUserNameForWorkspaceId(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');

        $this->assertSame('kbc-prefix-ws-123', $nameGenerator->createWorkspaceUserNameForWorkspaceId('123'));
    }

    public function testCreateDataExchangeId(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');
        $this->assertSame(
            'KBC_PREFIX_123_RO',
            $nameGenerator->createDataExchangeId('123'),
        );
    }
}
