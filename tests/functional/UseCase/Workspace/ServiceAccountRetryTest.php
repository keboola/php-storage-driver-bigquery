<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Google\Cloud\ResourceManager\V3\Project;
use Google\Cloud\ResourceManager\V3\ProjectsClient;
use Google\Service\Iam;
use Google_Service_Iam_CreateServiceAccountRequest;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\TooManyRequestsException;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception as CommonDriverException;

class ServiceAccountRetryTest extends BaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
    }

    public function skipCreateManyServiceAccounts(): void
    {
        $credentials = $this->getCredentials();
        $projectsClient = $this->clientManager->getProjectClient($credentials);
        $iamClient = $this->clientManager->getIamClient($credentials);

        $stackPrefix = (string) getenv('BQ_STACK_PREFIX');
        $folderId = (string) getenv('BQ_FOLDER_ID');

        $nameGenerator = new NameGenerator($stackPrefix);

        $projectId = $nameGenerator->createProjectId('-retry-' . time());

        $projectCreateResult = $this->createProjectForTest(
            $projectsClient,
            $folderId,
            $projectId
        );
        $projectName = $projectCreateResult->getName();

        $tooManyRequestsTested = false;
        $namePrefix = 'sa-' . time();
        // using very low initial delay to test that the retry will fail. catching the exception will happen later
        $iamClient->getClient()->setConfig(
            'retry',
            ['retries' => 3, 'factor' => 1.1, 'initial_delay' => 5, 'jitter' => 0.1]
        );
        for ($i = 1; $i < 10; $i++) {
            $projectServiceAccountId = $nameGenerator->createProjectServiceAccountId($namePrefix . '-' . $i);
            try {
                $iamClient->createServiceAccount($projectServiceAccountId,$projectName);
            } catch (TooManyRequestsException $e) {
                $tooManyRequestsTested = true;
                break;
            }
        }
        if (!$tooManyRequestsTested) {
            $this->fail('Too many requests not tested');
        }

        // using default retry setting to test that the retry will succeed
        $iamClient->getClient()->setConfig('retry', GCPClientManager::DEFAULT_RETRY_SETTINGS);
        $namePrefix = 'sa-' . time();
        for ($i = 1; $i < 10; $i++) {
            $projectServiceAccountId = $nameGenerator->createProjectServiceAccountId($namePrefix . '-' . $i);
            try {
                $this->createServiceAccount($iamClient, $projectServiceAccountId, $projectName);
            } catch (TooManyRequestsException $e) {
                $this->fail('This should not happen. Too many requests should be handled by retry.');
            }
        }
    }

    private function createServiceAccount(Iam $iamService, string $projectServiceAccountId, string $projectName): void
    {
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $createServiceAccountRequest = new Google_Service_Iam_CreateServiceAccountRequest();

        $createServiceAccountRequest->setAccountId($projectServiceAccountId);
        $serviceAccountsService->create($projectName, $createServiceAccountRequest);
    }

    // custom project creation because we need project name, should be cleaned by cleanTestProject()
    private function createProjectForTest(ProjectsClient $projectsClient, string $folderId, string $projectId): Project
    {
        $project = new Project();

        $project->setParent('folders/' . $folderId);
        $project->setProjectId($projectId);
        $project->setDisplayName($projectId);

        $operationResponse = $projectsClient->createProject($project);
        $operationResponse->pollUntilComplete();
        if ($operationResponse->operationSucceeded()) {
            /** @var Project $projectCreateResult */
            $projectCreateResult = $operationResponse->getResult();
        } else {
            $error = $operationResponse->getError();
            assert($error !== null);
            throw new CommonDriverException($error->getMessage(), $error->getCode());
        }

        return $projectCreateResult;
    }
}
