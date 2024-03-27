<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Create;

use Google\Service\CloudResourceManager;
use Google\Service\CloudResourceManager\GetIamPolicyRequest;
use Google\Service\CloudResourceManager\GetPolicyOptions;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use RuntimeException;
use Throwable;

class Helper
{
    public const REQUESTED_POLICY_VERSION = 3;

    public static function assertServiceAccountBindings(
        CloudResourceManager $cloudResourceManager,
        string $projectName,
        string $wsServiceAccEmail,
        LoggerInterface $logger,
    ): void {
        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) use ($logger) {
            $logger->debug('Try check iam policy Err:' . $e->getMessage());
            return true;
        }, 5);
        $backOffPolicy = new ExponentialRandomBackOffPolicy(
            10_000, // 10s
            1.5,
            120_000, // 2m
        );
        if (!str_starts_with($projectName, 'projects')) {
            $projectName = 'projects/' . $projectName;
        }

        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $proxy->call(function () use ($cloudResourceManager, $projectName, $wsServiceAccEmail, $logger): void {
            $logger->log(LogLevel::DEBUG, 'Try check iam policy for ' . $wsServiceAccEmail . ' in ' . $projectName);
            $getIamPolicyRequest = new GetIamPolicyRequest();
            $option = new GetPolicyOptions();
            $option->setRequestedPolicyVersion(self::REQUESTED_POLICY_VERSION);
            $getIamPolicyRequest->setOptions($option);
            $actualPolicy = $cloudResourceManager->projects->getIamPolicy($projectName, $getIamPolicyRequest);
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
                    implode(',', $serviceAccRoles),
                ));
            }
        });
    }
}
