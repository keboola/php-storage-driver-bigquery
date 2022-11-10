<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Project;

use Google\Protobuf\Any;
use Google\Service\Exception;
use Google_Service_CloudResourceManager_GetIamPolicyRequest;
use Google_Service_Iam_CreateServiceAccountRequest;
use Keboola\StorageDriver\BigQuery\GCPServiceIds;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Drop\DropProjectHandler;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\FunctionalTests\BaseCase;

class CreateDropProjectTest extends BaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testCreateProject(): void
    {
        $handler = new CreateProjectHandler($this->clientManager);
        $command = new CreateprojectCommand();

        $meta = new Any();
        $meta->pack((new CreateProjectCommand\CreateProjectBigqueryMeta())->setGcsFileBucketName(
            (string) getenv('BQ_BUCKET_NAME')
        ));
        $command->setStackPrefix($this->getStackPrefix());
        $command->setProjectId($this->getProjectId());
        $command->setMeta($meta);

        /** @var CreateProjectResponse $response */
        $response = $handler(
            $this->getCredentials(),
            $command,
            []
        );

        $this->assertInstanceOf(CreateProjectResponse::class, $response);

        $credentials = $this->getCredentials();
        $serviceUsageClient = $this->clientManager->getServiceUsageClient($credentials);

        /** @var array<string, string> $publicPart */
        $publicPart = (array) json_decode($response->getProjectUserName(), true, 512, JSON_THROW_ON_ERROR);

        $projectId = $publicPart['project_id'];

        $billingClient = $this->clientManager->getBillingClient($credentials);
        $billingInfo = $billingClient->getProjectBillingInfo('projects/'.$projectId);
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
        ];

        $this->assertEqualsArrays($expectedEnabledServices, $enabledServices);

        $cloudResourceManager = $this->clientManager->getCloudResourceManager($credentials);
        $actualPolicy = $cloudResourceManager->projects->getIamPolicy(
            'projects/' . $projectId,
            (new Google_Service_CloudResourceManager_GetIamPolicyRequest()),
            []
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
        ];
        $this->assertEqualsArrays($expected, $serviceAccRoles);

        $handler = new DropProjectHandler($this->clientManager);
        $command = (new DropProjectCommand())
            ->setProjectUserName($response->getProjectUserName());

        $handler(
            $this->getCredentials(),
            $command,
            []
        );

        $projectsClient = $this->clientManager->getProjectClient($this->getCredentials());
        $formattedName = $projectsClient->projectName($projectId);
        $removedProject = $projectsClient->getProject($formattedName);
        $this->assertTrue($removedProject->hasDeleteTime());

        $iamService = $this->clientManager->getIamClient($credentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $createServiceAccountRequest = new Google_Service_Iam_CreateServiceAccountRequest();

        $createServiceAccountRequest->setAccountId($publicPart['client_email']);

        $this->expectException(Exception::class);
        $this->expectExceptionCode(404);
        $serviceAccountsService->get(sprintf(
            'projects/%s/serviceAccounts/%s',
            $projectId,
            $publicPart['client_email']
        ));
    }
}
