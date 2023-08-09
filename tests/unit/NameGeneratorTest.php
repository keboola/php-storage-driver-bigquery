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

        $this->assertSame('in_c_bucket', $nameGenerator->createObjectNameForBucketInProject('in.c-bucket', null));
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

        $this->assertSame('kbc-prefix-workspace-123', $nameGenerator->createWorkspaceUserNameForWorkspaceId('123'));
    }

    public function testCreateDataExchangeId(): void
    {
        $nameGenerator = new NameGenerator('KBC_prefix_');
        $this->assertSame(
            'local_project_12_02_09_04_31_SHARE_exchanger',
            $nameGenerator->createDataExchangeId('local-project-12-02-09-04-31')
        );
        $this->assertSame('project_SHARE_exchanger', $nameGenerator->createDataExchangeId('project'));
    }
}
