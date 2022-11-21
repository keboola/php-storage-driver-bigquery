<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Google\Cloud\Core\Exception\ServiceException;
use Google\Service\Exception as GoogleServiceException;
use Google_Service_CloudResourceManager_GetIamPolicyRequest;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Drop\DropWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\IAmPermissions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Throwable;

class CreateDropWorkspaceTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$credentials, $response] = $this->createTestProject();
        $this->projectCredentials = $credentials;
        $this->projectResponse = $response;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testCreateDropWorkspace(): void
    {
        // CREATE
        [$credentials, $response] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);
        $this->assertInstanceOf(GenericBackendCredentials::class, $credentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $wsKeyData = CredentialsHelper::getCredentialsArray($credentials);
        $projectId = $wsKeyData['project_id'];
        $wsServiceAccEmail = $wsKeyData['client_email'];

        // check objects created
        $iamService = $this->clientManager->getIamClient($this->projectCredentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $wsServiceAcc = $serviceAccountsService->get(
            sprintf('projects/%s/serviceAccounts/%s', $projectId, $wsServiceAccEmail)
        );
        $this->assertNotNull($wsServiceAcc);

        $wsBqClient = $this->clientManager->getBigQueryClient($credentials);

        /** @var array<string, string> $datasets */
        $datasets = $bqClient->runQuery($bqClient->query(sprintf('SELECT
  schema_name
FROM
  %s.INFORMATION_SCHEMA.SCHEMATA;', BigqueryQuote::quoteSingleIdentifier($projectId))))->getIterator()->current();

        $this->assertSame(
            strtoupper($response->getWorkspaceObjectName()),
            $datasets['schema_name']
        );

        // test ws service acc is owner of ws dataset
        $workspaceDataset = $bqClient->dataset($response->getWorkspaceObjectName())->info();
        $this->assertNotNull($workspaceDataset);
        $this->assertCount(1, $workspaceDataset['access']);
        $this->assertSame('OWNER', $workspaceDataset['access'][0]['role']);
        $this->assertSame($wsServiceAccEmail, $workspaceDataset['access'][0]['userByEmail']);

        $cloudResourceManager = $this->clientManager->getCloudResourceManager($this->projectCredentials);
        $actualPolicy = $cloudResourceManager->projects->getIamPolicy(
            'projects/' . $projectId,
            (new Google_Service_CloudResourceManager_GetIamPolicyRequest()),
            []
        );
        $actualPolicy = $actualPolicy->getBindings();

        $serviceAccRoles = [];
        foreach ($actualPolicy as $policy) {
            if (in_array('serviceAccount:' . $wsServiceAccEmail, $policy->getMembers())) {
                $serviceAccRoles[] = $policy->getRole();
            }
        }

        // ws service acc must have a job user role to be able to run queries
        $expected = [
            IAmPermissions::ROLES_BIGQUERY_DATA_VIEWER, // readOnly access
            IAmPermissions::ROLES_BIGQUERY_JOB_USER,
        ];
        $this->assertEqualsArrays($expected, $serviceAccRoles);

        try {
            $wsBqClient->runQuery($wsBqClient->query(sprintf(
                'CREATE SCHEMA `should_fail`',
            )));
            $this->fail('The workspace user should not have the right to create a new dataset.');
        } catch (ServiceException $exception) {
            $this->assertSame(403, $exception->getCode());
            $this->assertStringContainsString(
                'User does not have bigquery.datasets.create permission in project',
                $exception->getMessage()
            );
        }

        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE SCHEMA `testReadOnlySchema`;',
        )));

        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE TABLE `testReadOnlySchema`.`testTable` (`id` INTEGER);',
        )));

        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT `testReadOnlySchema`.`testTable` (`id`) VALUES (1), (2), (3);',
        )));

        $result = $wsBqClient->runQuery($wsBqClient->query('SELECT * FROM `testReadOnlySchema`.`testTable`;'));

        $this->assertCount(3, $result);
        // try to create table
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'CREATE TABLE %s.`testTable` (`id` INTEGER);',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        // try to create view
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'CREATE VIEW %s.`testView` AS '
            . 'SELECT `id` FROM %s.`testTable`;',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName()),
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        // try to drop view
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'DROP VIEW %s.`testView`;',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        // try to drop table
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'DROP TABLE %s.`testTable`;',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        // DROP
        $handler = new DropWorkspaceHandler($this->clientManager);
        $command = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName())
            ->setWorkspaceRoleName($response->getWorkspaceRoleName())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());

        $dropResponse = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        $this->assertNull($dropResponse);

        try {
            $serviceAccountsService->get(sprintf('projects/%s/serviceAccounts/%s', $projectId, $wsServiceAccEmail));
            $this->fail('Service account should be deleted.');
        } catch (GoogleServiceException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertStringContainsString('.iam.gserviceaccount.com does not exist.', $e->getMessage());
        }

        $datasets = $bqClient->runQuery(
            $bqClient->query(sprintf(
                'SELECT schema_name FROM %s.INFORMATION_SCHEMA.SCHEMATA WHERE `schema_name` = %s;',
                BigqueryQuote::quoteSingleIdentifier($projectId),
                BigqueryQuote::quote($response->getWorkspaceObjectName())
            ))
        );

        $this->assertNull($datasets->getIterator()->current());
    }

    public function testCreateDropCascadeWorkspace(): void
    {
        // CREATE
        [$credentials, $response] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);
        $this->assertInstanceOf(GenericBackendCredentials::class, $credentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $wsBqClient = $this->clientManager->getBigQueryClient($credentials);

        // create table
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'CREATE TABLE %s.`testTable` (`id` INTEGER);',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        $projectBqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);

        // try to DROP - should fail, there is a table
        $handler = new DropWorkspaceHandler($this->clientManager);
        $command = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName())
            ->setWorkspaceRoleName($response->getWorkspaceRoleName())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());
        try {
            $handler(
                $this->projectCredentials,
                $command,
                []
            );
            $this->fail('Should fail as workspace database contains table');
        } catch (Throwable $e) {
            $this->assertStringContainsString(
                'is still in use',
                $e->getMessage()
            );
        }

        $wsKeyData = CredentialsHelper::getCredentialsArray($credentials);
        $projectId = $wsKeyData['project_id'];
        $wsServiceAccEmail = $wsKeyData['client_email'];

        /** @var array<string, string> $datasets */
        $datasets = $projectBqClient->runQuery($projectBqClient->query(sprintf('SELECT
  schema_name
FROM
  %s.INFORMATION_SCHEMA.SCHEMATA;', BigqueryQuote::quoteSingleIdentifier($projectId))))->getIterator()->current();

        // ws dataset exist
        $this->assertSame(
            strtoupper($response->getWorkspaceObjectName()),
            $datasets['schema_name']
        );

        // check if ws service acc still exist
        $iamService = $this->clientManager->getIamClient($this->projectCredentials);
        $serviceAccountsService = $iamService->projects_serviceAccounts;
        $wsServiceAcc = $serviceAccountsService->get(sprintf(
            'projects/%s/serviceAccounts/%s',
            $projectId,
            $wsServiceAccEmail
        ));
        $this->assertNotNull($wsServiceAcc);

        // try to DROP - should not fail and database will be deleted
        $command->setIsCascade(true);
        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        try {
            $serviceAccountsService->get(sprintf('projects/%s/serviceAccounts/%s', $projectId, $wsServiceAccEmail));
            $this->fail('Service account should be deleted.');
        } catch (GoogleServiceException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertStringContainsString('.iam.gserviceaccount.com does not exist.', $e->getMessage());
        }

        $datasets = $projectBqClient->runQuery($projectBqClient->query(sprintf('SELECT
  schema_name
FROM
  %s.INFORMATION_SCHEMA.SCHEMATA;', BigqueryQuote::quoteSingleIdentifier($projectId))));

        $this->assertNull($datasets->getIterator()->current());
    }
}
