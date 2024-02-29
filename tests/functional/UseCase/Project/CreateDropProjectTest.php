<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Project;

use Google\Protobuf\Any;
use Google\Service\Exception;
use Google_Service_CloudResourceManager_GetIamPolicyRequest;
use Google_Service_Iam_CreateServiceAccountRequest;
use Keboola\StorageDriver\BigQuery\GCPServiceIds;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\ProjectIdTooLongException;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\ProjectWithProjectIdAlreadyExists;
use Keboola\StorageDriver\BigQuery\Handler\Project\Drop\DropProjectHandler;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\FunctionalTests\BaseCase;

class CreateDropProjectTest extends BaseCase
{
    private string $rand = '';

    protected function getProjectId(): string
    {
        return 'prj-' . $this->rand;
    }

    private function getCurrentProjectFullId(): string
    {
        return (new NameGenerator($this->getStackPrefix()))->createProjectId($this->getProjectId() . $this->rand);
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->rand = self::getRand();
        $this->dropProjects($this->getCurrentProjectFullId());
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->dropProjects($this->getCurrentProjectFullId());
    }

    public function testCreateProject(): void
    {
        $handler = new CreateProjectHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new CreateProjectCommand();

        $meta = new Any();
        $fileStorageBucketName = (string) getenv('BQ_BUCKET_NAME');
        $meta->pack(
            (new CreateProjectCommand\CreateProjectBigqueryMeta())
                ->setGcsFileBucketName($fileStorageBucketName)
                ->setRegion(BaseCase::DEFAULT_LOCATION),
        );
        $command->setStackPrefix($this->getStackPrefix());
        $command->setProjectId($this->getProjectId());
        $command->setMeta($meta);

        $this->log->add($command->serializeToJsonString());
        $this->log->add((new NameGenerator($command->getStackPrefix()))->createProjectId($command->getProjectId()));
        /** @var CreateProjectResponse $response */
        $response = $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(CreateProjectResponse::class, $response);

        $credentials = $this->getCredentials();
        $serviceUsageClient = $this->clientManager->getServiceUsageClient($credentials);

        /** @var array<string, string> $publicPart */
        $publicPart = (array) json_decode($response->getProjectUserName(), true, 512, JSON_THROW_ON_ERROR);

        $projectId = $publicPart['project_id'];

        $billingClient = $this->clientManager->getBillingClient($credentials);
        $billingInfo = $billingClient->getProjectBillingInfo('projects/' . $projectId);
        $this->assertNotEmpty($billingInfo->getBillingAccountName());

        $pagedResponse = $serviceUsageClient->listServices([
            'parent' => 'projects/' . $projectId,
            'filter' => 'state:ENABLED',
        ]);

        $enabledServices = [];
        foreach ($pagedResponse->iteratePages() as $page) {
            /** @var \Google\Cloud\ServiceUsage\V1\Service $element */
            foreach ($page as $element) {
                $this->assertNotNull($element->getConfig());
                $enabledServices[] = $element->getConfig()->getName();
            }
        }

        $expectedEnabledServices = [
            GCPServiceIds::IAM_SERVICE,
            GCPServiceIds::IAM_CREDENTIALS_SERVICE,
            GCPServiceIds::BIGQUERY_SERVICE,
            GCPServiceIds::BIGQUERY_MIGRATION_SERVICE,
            GCPServiceIds::BIGQUERY_STORAGE_SERVICE,
            GCPServiceIds::SERVICE_USAGE_SERVICE,
            GCPServiceIds::CLOUD_BILLING_SERVICE,
            GCPServiceIds::CLOUD_RESOURCE_MANAGER_SERVICE,
            GCPServiceIds::CLOUD_ANALYTIC_HUB_SERVICE,
        ];

        $this->assertSame(
            array_diff($expectedEnabledServices, $enabledServices),
            [],
            sprintf(
                'Services "%s" are missing in enabled services.' . PHP_EOL . 'Expected: %s' . PHP_EOL . 'Actual: %s',
                implode(', ', array_diff($expectedEnabledServices, $enabledServices)),
                implode(', ', $expectedEnabledServices),
                implode(', ', $enabledServices),
            ),
        );

        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        $actualPolicy = $cloudResourceManager->projects->getIamPolicy(
            'projects/' . $projectId,
            (new Google_Service_CloudResourceManager_GetIamPolicyRequest()),
            [],
        );
        $actualPolicy = $actualPolicy->getBindings();

        $serviceAccRoles = [];
        foreach ($actualPolicy as $policy) {
            if (in_array('serviceAccount:' . $publicPart['client_email'], $policy->getMembers())) {
                $serviceAccRoles[] = $policy->getRole();
            }
        }
        $expected = [
            IAmPermissions::ROLES_BIGQUERY_DATA_OWNER,
            IAmPermissions::ROLES_BIGQUERY_JOB_USER,
            IAmPermissions::ROLES_IAM_SERVICE_ACCOUNT_CREATOR,
            IAmPermissions::ROLES_OWNER,
        ];
        $this->assertEqualsArrays($expected, $serviceAccRoles);

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);
        $location = BaseCase::DEFAULT_LOCATION;
        $dataExchangeId = $response->getProjectReadOnlyRoleName();
        $formattedName = $analyticHubClient->dataExchangeName($projectId, $location, $dataExchangeId);
        $readOnlyExchanger = $analyticHubClient->getDataExchange($formattedName);
        $this->assertNotNull($readOnlyExchanger);

        $storageManager = $this->clientManager->getStorageClient($credentials);
        $fileStorageBucket = $storageManager->bucket($fileStorageBucketName);
        $policy = $fileStorageBucket->iam()->policy();
        $hasStorageObjAdminRole = false;
        foreach ($policy['bindings'] as $binding) {
            if ($binding['role'] === 'roles/storage.objectAdmin') {
                $key = array_search('serviceAccount:' . $publicPart['client_email'], $binding['members']);
                if ($key !== false) {
                    $hasStorageObjAdminRole = true;
                }
            }
        }
        $this->assertTrue($hasStorageObjAdminRole);

        $handler = new DropProjectHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $meta = new Any();
        $meta->pack(
            (new DropProjectCommand\DropProjectBigqueryMeta())
                ->setGcsFileBucketName($fileStorageBucketName,)
                ->setRegion(BaseCase::DEFAULT_LOCATION),
        );
        $command = (new DropProjectCommand())
            ->setProjectUserName($response->getProjectUserName())
            ->setReadOnlyRoleName($response->getProjectReadOnlyRoleName())
            ->setMeta($meta);

        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $projectsClient = $this->clientManager->getProjectClient($this->getCredentials());
        $formattedName = $projectsClient->projectName($projectId);
        $removedProject = $projectsClient->getProject($formattedName);
        $this->assertTrue($removedProject->hasDeleteTime());

        $iamService = $this->clientManager->getIamClient($credentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $createServiceAccountRequest = new Google_Service_Iam_CreateServiceAccountRequest();

        $createServiceAccountRequest->setAccountId($publicPart['client_email']);

        $storageManager = $this->clientManager->getStorageClient($credentials);
        $fileStorageBucket = $storageManager->bucket($fileStorageBucketName);
        $policy = $fileStorageBucket->iam()->policy();
        $hasStorageObjAdminRole = false;
        foreach ($policy['bindings'] as $binding) {
            if ($binding['role'] === 'roles/storage.objectAdmin') {
                $key = array_search('serviceAccount:' . $publicPart['client_email'], $binding['members']);
                if ($key) {
                    $hasStorageObjAdminRole = true;
                }
            }
        }
        $this->assertFalse($hasStorageObjAdminRole);

        $this->expectException(Exception::class);
        $this->expectExceptionCode(404);
        $serviceAccountsService->get(sprintf(
            'projects/%s/serviceAccounts/%s',
            $projectId,
            $publicPart['client_email'],
        ));
    }

    public function testCreateProjectEdgeCases(): void
    {
        $handler = new CreateProjectHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new CreateProjectCommand();

        $meta = new Any();
        $fileStorageBucketName = (string) getenv('BQ_BUCKET_NAME');
        $meta->pack(
            (new CreateProjectCommand\CreateProjectBigqueryMeta())
                ->setGcsFileBucketName($fileStorageBucketName)
                ->setRegion(BaseCase::DEFAULT_LOCATION),
        );
        $command->setStackPrefix($this->getStackPrefix());
        $command->setProjectId($this->getProjectId() . '1234567890123456789');
        $command->setMeta($meta);

        $this->log->add($command->serializeToJsonString());
        $this->log->add(
            (new NameGenerator($command->getStackPrefix()))
                ->createProjectId($this->getProjectId() . '1234567890'),
        );

        // test long project id
        try {
            $handler(
                $this->getCredentials(),
                $command,
                [],
                new RuntimeOptions(),
            );
            $this->fail('Should fail because project id is too long.');
        } catch (ProjectIdTooLongException $e) {
            $this->assertStringContainsString('It must be at most 30 characters long.', $e->getMessage());
            $this->assertSame(3000, $e->getCode());
        }

        // run twice to get ProjectWithProjectIdAlreadyExists
        $command->setProjectId($this->getProjectId());
        $this->log->add($command->serializeToJsonString());
        $this->log->add((new NameGenerator($command->getStackPrefix()))->createProjectId($this->getProjectId()));

        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        try {
            $handler(
                $this->getCredentials(),
                $command,
                [],
                new RuntimeOptions(),
            );
        } catch (ProjectWithProjectIdAlreadyExists $e) {
            $this->assertStringContainsString('already exists', $e->getMessage());
            $this->assertSame(2006, $e->getCode());
        }
    }
}
