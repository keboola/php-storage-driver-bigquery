<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Google\Cloud\Core\Exception\BadRequestException;
use Google\Cloud\Core\Exception\ServiceException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\ResetPassword\ResetWorkspacePasswordHandler;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordCommand;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Throwable;

class ResetWorkspacePasswordTest extends BaseCase
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

    public function testResetWorkspacePassword(): void
    {
        // create workspace
        [$credentials, $createResponse] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);
        assert($credentials instanceof GenericBackendCredentials);
        assert($createResponse instanceof CreateWorkspaceResponse);

        // reset password
        $handler = new ResetWorkspacePasswordHandler($this->clientManager);
        $command = (new ResetWorkspacePasswordCommand())
            ->setWorkspaceUserName($createResponse->getWorkspaceUserName());

        $passwordResponse = $handler(
            $this->projectCredentials,
            $command,
            []
        );
        assert($passwordResponse instanceof ResetWorkspacePasswordResponse);

        $wsBqClient = $this->clientManager->getBigQueryClient($credentials);
        try {
            $wsBqClient->runQuery($wsBqClient->query('SELECT 1'));
            $this->fail('Should fail');
        } catch (Throwable $e) {
            $this->assertInstanceOf(ServiceException::class, $e);
        }

        // check new password
        $credentials->setPrincipal($passwordResponse->getWorkspaceUserName());
        $credentials->setSecret($passwordResponse->getWorkspacePassword());

        $wsBqClient = $this->clientManager->getBigQueryClient($credentials);
        /** @var array<string, string> $result */
        $result = $wsBqClient->runQuery(
            $wsBqClient->query('SELECT SESSION_USER() AS USER')
        )->getIterator()->current();

        $credentialsArr = (array) json_decode($createResponse->getWorkspaceUserName(), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame($credentialsArr['client_email'], $result['USER']);
    }
}
