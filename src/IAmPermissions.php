<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

final class IAmPermissions
{
    public const RESOURCE_MANAGER_PROJECTS_CREATE = 'resourcemanager.projects.create';

    public const ROLES_BIGQUERY_DATA_OWNER = 'roles/bigquery.dataOwner';
    public const ROLES_BIGQUERY_DATA_VIEWER = 'roles/bigquery.dataViewer';
    public const ROLES_BIGQUERY_JOB_USER = 'roles/bigquery.jobUser';
    public const ROLES_IAM_SERVICE_ACCOUNT_CREATOR = 'roles/iam.serviceAccountCreator';
    public const ROLES_OWNER = 'roles/owner';
}
