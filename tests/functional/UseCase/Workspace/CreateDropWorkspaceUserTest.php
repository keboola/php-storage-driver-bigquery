<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Google\Cloud\Core\Exception\ServiceException;
use Google\Protobuf\Any;
use Google\Service\CloudResourceManager\GetIamPolicyRequest;
use Google\Service\Exception as GoogleServiceException;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\Helper;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\CreateUser\CreateWorkspaceUserHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\DropUser\DropWorkspaceUserHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceUserCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceUserResponse;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceUserCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

/**
 * @group sync
 */
class CreateDropWorkspaceUserTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];
        $this->projectResponse = $this->projects[0][1];
    }

    public function testCreateDropWorkspaceUser(): void
    {
        // First create a workspace (dataset + service account)
        [
            $wsCredentials,
            $wsResponse,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);

        $this->assertInstanceOf(GenericBackendCredentials::class, $wsCredentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $wsResponse);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $projectCredentials = CredentialsHelper::getCredentialsArray($this->projectCredentials);
        $projectId = $projectCredentials['project_id'];

        // CREATE workspace user - this creates a new service account with access to existing workspace dataset
        $handler = new CreateWorkspaceUserHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $command = (new CreateWorkspaceUserCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setWorkspaceId($wsResponse->getWorkspaceObjectName())
            ->setWorkspaceObjectName($wsResponse->getWorkspaceObjectName())
            ->setProjectReadOnlyRoleName($this->projectResponse->getProjectReadOnlyRoleName());

        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(CreateWorkspaceUserResponse::class, $response);
        $this->assertNotEmpty($response->getWorkspaceUserName());
        $this->assertNotEmpty($response->getWorkspacePassword());

        // Parse the new user's credentials
        /** @var array<string, string> $newUserKeyData */
        $newUserKeyData = json_decode($response->getWorkspaceUserName(), true, 512, JSON_THROW_ON_ERROR);
        $newUserEmail = $newUserKeyData['client_email'];

        // Verify the new service account exists
        $iamService = $this->clientManager->getIamClient($this->projectCredentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $newServiceAcc = $serviceAccountsService->get(
            sprintf('projects/%s/serviceAccounts/%s', $projectId, $newUserEmail),
        );
        $this->assertNotNull($newServiceAcc);

        // Verify the new user has OWNER access on the workspace dataset
        /** @var array<string, mixed> $workspaceDataset */
        $workspaceDataset = $bqClient->dataset($wsResponse->getWorkspaceObjectName())->info();
        /** @var list<array<string, mixed>> $accessList */
        $accessList = $workspaceDataset['access'] ?? [];
        $newUserHasAccess = false;
        foreach ($accessList as $accessEntry) {
            if (isset($accessEntry['userByEmail']) && $accessEntry['userByEmail'] === $newUserEmail) {
                $this->assertSame('OWNER', $accessEntry['role']);
                $newUserHasAccess = true;
            }
        }
        $this->assertTrue($newUserHasAccess, 'New workspace user should have OWNER access on workspace dataset');

        // Verify the new user has proper IAM bindings
        Helper::assertServiceAccountBindings(
            $this->clientManager->getCloudResourceManager($this->projectCredentials),
            'projects/' . $projectId,
            $newUserEmail,
            $this->log,
        );

        // Build credentials for the new workspace user
        $meta = new Any();
        $meta->pack(
            (new GenericBackendCredentials\BigQueryCredentialsMeta())
                ->setRegion(self::DEFAULT_LOCATION),
        );
        $newUserCredentials = (new GenericBackendCredentials())
            ->setHost($this->projectCredentials->getHost())
            ->setPrincipal($response->getWorkspaceUserName())
            ->setSecret($response->getWorkspacePassword())
            ->setPort($this->projectCredentials->getPort());
        $newUserCredentials->setMeta($meta);

        // Verify the new user can query the workspace dataset
        $newUserBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $newUserCredentials);

        // Create a table with the new user in the workspace
        $newUserBqClient->runQuery($newUserBqClient->query(sprintf(
            'CREATE TABLE %s.`test_ws_user_table` (`id` INTEGER)',
            BigqueryQuote::quoteSingleIdentifier($wsResponse->getWorkspaceObjectName()),
        )));

        // Verify the new user can read from the table
        $result = $newUserBqClient->runQuery($newUserBqClient->query(sprintf(
            'SELECT * FROM %s.`test_ws_user_table`',
            BigqueryQuote::quoteSingleIdentifier($wsResponse->getWorkspaceObjectName()),
        )));
        $this->assertCount(0, $result);

        // Clean up table
        $newUserBqClient->runQuery($newUserBqClient->query(sprintf(
            'DROP TABLE %s.`test_ws_user_table`',
            BigqueryQuote::quoteSingleIdentifier($wsResponse->getWorkspaceObjectName()),
        )));

        // DROP workspace user
        $dropHandler = new DropWorkspaceUserHandler($this->clientManager);
        $dropHandler->setInternalLogger($this->log);

        $dropCommand = (new DropWorkspaceUserCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName());

        $dropResponse = $dropHandler(
            $this->projectCredentials,
            $dropCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertNull($dropResponse);

        // Verify the service account has been deleted
        try {
            $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
                if ($e->getMessage() === 'Service account should be deleted.') {
                    return true;
                }
                return false;
            });
            $proxy = new RetryProxy($retryPolicy, new ExponentialBackOffPolicy());
            $proxy->call(function () use ($serviceAccountsService, $projectId, $newUserEmail): void {
                $serviceAccountsService->get(
                    sprintf('projects/%s/serviceAccounts/%s', $projectId, $newUserEmail),
                );
                $this->fail('Service account should be deleted.');
            });
        } catch (GoogleServiceException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertStringContainsString('.iam.gserviceaccount.com does not exist.', $e->getMessage());
        }

        // Verify IAM policies have been cleaned up
        $cloudResourceManager = $this->clientManager->getCloudResourceManager($this->projectCredentials);
        /** @var \Google\Service\CloudResourceManager\Resource\Projects $projects */
        $projects = $cloudResourceManager->projects;
        /** @var \Google\Service\CloudResourceManager\Policy $actualPolicy */
        $actualPolicy = $projects->getIamPolicy(
            'projects/' . $projectId,
            (new GetIamPolicyRequest()),
            [],
        );

        /** @var \Google\Service\CloudResourceManager\Binding $binding */
        foreach ($actualPolicy->getBindings() as $binding) {
            /** @var string[] $members */
            $members = $binding->getMembers();
            $this->assertNotContains(
                'serviceAccount:' . $newUserEmail,
                $members,
                sprintf(
                    'Service account %s should be removed from IAM binding for role %s',
                    $newUserEmail,
                    $binding->getRole(),
                ),
            );
        }
    }
}
