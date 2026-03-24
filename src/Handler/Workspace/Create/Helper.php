<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Create;

use Google\Service\CloudResourceManager;
use Google\Service\CloudResourceManager\Binding;
use Google\Service\CloudResourceManager\Expr;
use Google\Service\CloudResourceManager\GetIamPolicyRequest;
use Google\Service\CloudResourceManager\GetPolicyOptions;
use Google\Service\CloudResourceManager\Policy;
use Google\Service\CloudResourceManager\SetIamPolicyRequest;
use Google\Service\Iam\ServiceAccount;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\BigQuery\IAMServiceWrapper;
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

    public static function createServiceAccount(
        IAMServiceWrapper $iamService,
        string $serviceAccountId,
        string $projectName,
        LoggerInterface $logger,
    ): ServiceAccount {
        $newServiceAccountName = sprintf(
            '%s/serviceAccounts/%s@%s.iam.gserviceaccount.com',
            $projectName,
            $serviceAccountId,
            str_replace('projects/', '', $projectName),
        );

        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) use ($logger) {
            $logger->debug('Try create SA Err:' . $e->getMessage());
            return true;
        }, 5);
        $backOffPolicy = new ExponentialRandomBackOffPolicy(
            5_000, // 5s
            1.8,
            60_000, // 1m
        );
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);
        $wsServiceAcc = $proxy->call(function () use (
            $iamService,
            $newServiceAccountName,
            $serviceAccountId,
            $projectName,
        ): ServiceAccount {
            try {
                return $iamService->projects_serviceAccounts->get($newServiceAccountName);
            } catch (Throwable) {
                $iamService->createServiceAccount($serviceAccountId, $projectName);
            }
            return $iamService->projects_serviceAccounts->get($newServiceAccountName);
        });
        assert($wsServiceAcc instanceof ServiceAccount);

        return $wsServiceAcc;
    }

    public static function grantProjectIamRoles(
        CloudResourceManager $cloudResourceManager,
        string $projectName,
        ServiceAccount $wsServiceAcc,
        LoggerInterface $logger,
        bool $includeDataViewer = true,
    ): void {
        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) use ($logger) {
            $logger->debug('Try set iam policy Err:' . $e->getMessage());
            return true;
        }, 10);
        $backOffPolicy = new ExponentialRandomBackOffPolicy(
            5_000, // 5s
            1.8,
            60_000, // 1m
        );
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy);

        $proxy->call(function () use ($cloudResourceManager, $projectName, $wsServiceAcc, $logger, $includeDataViewer): void {
            $getIamPolicyRequest = new GetIamPolicyRequest();
            $option = new GetPolicyOptions();
            $option->setRequestedPolicyVersion(self::REQUESTED_POLICY_VERSION);
            $getIamPolicyRequest->setOptions($option);
            /** @var \Google\Service\CloudResourceManager\Resource\Projects $projects */
            $projects = $cloudResourceManager->projects;
            /** @var Policy $actualPolicy */
            $actualPolicy = $projects->getIamPolicy($projectName, $getIamPolicyRequest);
            $finalBinding[] = $actualPolicy->getBindings();

            foreach (CreateWorkspaceHandler::IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES as $role) {
                if ($role === IAmPermissions::ROLES_BIGQUERY_DATA_VIEWER && !$includeDataViewer) {
                    continue;
                }

                $bigQueryJobUserBinding = new Binding();
                $bigQueryJobUserBinding->setMembers('serviceAccount:' . $wsServiceAcc->getEmail());
                $bigQueryJobUserBinding->setRole($role);

                if ($role === IAmPermissions::ROLES_BIGQUERY_DATA_VIEWER) {
                    $conditionExpression = new Expr();
                    $conditionExpression->setTitle('ReadOnly Role');
                    $conditionExpression->setDescription('Allow read only from buckets not from other ws');
                    $conditionExpression->setExpression(sprintf(
                        "!resource.name.startsWith('%s/datasets/WORKSPACE_')",
                        $projectName,
                    ));
                    $bigQueryJobUserBinding->setCondition($conditionExpression);
                }
                $finalBinding[] = $bigQueryJobUserBinding;
            }

            $policy = new Policy();
            $policy->setVersion(self::REQUESTED_POLICY_VERSION);
            $policy->setEtag($actualPolicy->getEtag());
            $policy->setBindings($finalBinding);
            $setIamPolicyRequest = new SetIamPolicyRequest();
            $setIamPolicyRequest->setPolicy($policy);

            $logger->log(
                LogLevel::DEBUG,
                'Try set iam policy for ' . $wsServiceAcc->getEmail() . ' in ' . $projectName,
            );
            $projects->setIamPolicy($projectName, $setIamPolicyRequest);
            self::assertServiceAccountBindings(
                $cloudResourceManager,
                $projectName,
                $wsServiceAcc->getEmail(),
                $logger,
                $includeDataViewer,
            );
        });
    }

    public static function assertServiceAccountBindings(
        CloudResourceManager $cloudResourceManager,
        string $projectName,
        string $wsServiceAccEmail,
        LoggerInterface $logger,
        bool $includeDataViewer = true,
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
        $proxy->call(function () use ($cloudResourceManager, $projectName, $wsServiceAccEmail, $logger, $includeDataViewer): void {
            $logger->log(LogLevel::DEBUG, 'Try check iam policy for ' . $wsServiceAccEmail . ' in ' . $projectName);
            $getIamPolicyRequest = new GetIamPolicyRequest();
            $option = new GetPolicyOptions();
            $option->setRequestedPolicyVersion(self::REQUESTED_POLICY_VERSION);
            $getIamPolicyRequest->setOptions($option);
            /** @var \Google\Service\CloudResourceManager\Resource\Projects $projects */
            $projects = $cloudResourceManager->projects;
            /** @var \Google\Service\CloudResourceManager\Policy $actualPolicyResponse */
            $actualPolicyResponse = $projects->getIamPolicy($projectName, $getIamPolicyRequest);
            /** @var \Google\Service\CloudResourceManager\Binding[] $actualBindings */
            $actualBindings = $actualPolicyResponse->getBindings();

            $serviceAccRoles = [];
            foreach ($actualBindings as $policy) {
                /** @var string[] $policyMembers */
                $policyMembers = $policy->getMembers();
                if (in_array('serviceAccount:' . $wsServiceAccEmail, $policyMembers)) {
                    $serviceAccRoles[] = (string) $policy->getRole();
                }
            }

            sort($serviceAccRoles);

            // ws service acc must have a job user role to be able to run queries
            $expectedRoles = $includeDataViewer
                ? CreateWorkspaceHandler::IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES
                : array_values(array_filter(
                    CreateWorkspaceHandler::IAM_WORKSPACE_SERVICE_ACCOUNT_ROLES,
                    static fn(string $role) => $role !== IAmPermissions::ROLES_BIGQUERY_DATA_VIEWER,
                ));
            if ($serviceAccRoles !== $expectedRoles) {
                throw new RuntimeException(sprintf(
                    'Workspace service account has incorrect roles. Expected roles: [%s], actual roles: [%s]',
                    implode(',', $expectedRoles),
                    implode(',', $serviceAccRoles),
                ));
            }
        });
    }
}
