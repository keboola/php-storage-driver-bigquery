<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Create;

use Google\Service\CloudResourceManager;
use Google\Service\CloudResourceManager\GetIamPolicyRequest;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use RuntimeException;

class Helper
{
    public static function assertServiceAccountBindings(
        CloudResourceManager $cloudResourceManager,
        string $projectName,
        string $wsServiceAccEmail,
        LoggerInterface $logger,
    ): void {
        $retryPolicy = new SimpleRetryPolicy(10);
        $backOffPolicy = new ExponentialBackOffPolicy(
            initialInterval: 30_000, // 30s
            multiplier: 1.2, // 180s
            maxInterval: 180_000, // 30s
        );
        if (!str_starts_with($projectName, 'projects')) {
            $projectName = 'projects/' . $projectName;
        }

        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $proxy->call(function () use ($cloudResourceManager, $projectName, $wsServiceAccEmail, $logger): void {
            $logger->log(LogLevel::DEBUG, 'Try check iam policy');
            $actualPolicy = $cloudResourceManager->projects->getIamPolicy($projectName, (new GetIamPolicyRequest()));
            $actualPolicy = $actualPolicy->getBindings();

            $serviceAccRoles = [];
            foreach ($actualPolicy as $policy) {
                if (in_array('serviceAccount:' . $wsServiceAccEmail, $policy->getMembers())) {
                    $serviceAccRoles[] = $policy->getRole();
                }
            }

            sort($serviceAccRoles);

            // ws service acc must have a job user role to be able to run queries
            if ($serviceAccRoles !== CreateWorkspaceHandler::IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES) {
                throw new RuntimeException(sprintf(
                    'Workspace service account has incorrect roles. Expected roles: [%s], actual roles: [%s]',
                    implode(',', CreateWorkspaceHandler::IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES),
                    implode(',', $serviceAccRoles)
                ));
            }
        });
    }
}
