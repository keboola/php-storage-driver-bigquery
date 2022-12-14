<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

class NameGenerator
{
    protected string $stackPrefix;

    public function __construct(
        string $stackPrefix
    ) {
        $stackPrefix = rtrim($stackPrefix, '_');
        $stackPrefix = str_replace('_', '-', strtolower($stackPrefix));
        $this->stackPrefix = $stackPrefix;
    }

    public function createProjectId(string $projectId): string
    {
        return sprintf('%s-%s', $this->stackPrefix, $projectId);
    }

    public function createProjectServiceAccountId(string $projectId): string
    {
        return sprintf('%s-%s', $this->stackPrefix, $projectId);
    }

    public function createObjectNameForBucketInProject(string $bucketId): string
    {
        return str_replace(['.', '-'], '_', $bucketId);
    }

    public function createWorkspaceObjectNameForWorkspaceId(string $workspaceId): string
    {
        return str_replace('-', '_', strtoupper('workspace_' . $workspaceId));
    }

    public function createWorkspaceUserNameForWorkspaceId(string $workspaceId): string
    {
        return str_replace('_', '-', strtolower($this->createWorkspaceCredentialsPrefix($workspaceId)));
    }

    private function createWorkspaceCredentialsPrefix(string $workspaceId): string
    {
        return $this->stackPrefix . '-workspace-' . $workspaceId;
    }

    public function createDataExchangeId(string $projectStringId): string
    {
        return str_replace('-', '_', $projectStringId) . '_SHARE_exchanger';
    }
}
