<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Google\Cloud\Core\Exception\ServiceException;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\ResetPassword\ResetWorkspacePasswordHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordCommand;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

class ResetWorkspacePasswordTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];
        $this->projectResponse = $this->projects[0][1];
    }

    public function testResetWorkspacePassword(): void
    {
        // create workspace
        [
            $credentials,
            $createResponse,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);
        assert($credentials instanceof GenericBackendCredentials);
        assert($createResponse instanceof CreateWorkspaceResponse);

        // reset password
        $handler = new ResetWorkspacePasswordHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ResetWorkspacePasswordCommand())
            ->setWorkspaceUserName($createResponse->getWorkspaceUserName());

        $passwordResponse = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );
        assert($passwordResponse instanceof ResetWorkspacePasswordResponse);

        // Old credentials may still work briefly due to GCP eventual consistency,
        // so retry until the old key is invalidated
        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            // retry when old credentials still work (fail() throws AssertionFailedError)
            return $e->getMessage() === 'Should fail';
        }, 10);
        $proxy = new RetryProxy($retryPolicy, new ExponentialBackOffPolicy(10000));
        $proxy->call(function () use ($credentials): void {
            $this->clientManager->close();
            $wsBqClient = $this->clientManager->getBigQueryClient(
                $this->testRunId,
                $credentials,
                [],
                null,
                1,
            );
            try {
                $wsBqClient->runQuery($wsBqClient->query('SELECT 1'));
                $this->fail('Should fail');
            } catch (ServiceException $e) {
                $this->assertSame(401, $e->getCode());
            }
        });

        // check new password
        $credentials->setPrincipal($passwordResponse->getWorkspaceUserName());
        $credentials->setSecret($passwordResponse->getWorkspacePassword());

        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $credentials);
        /** @var array<string, string> $result */
        $result = $wsBqClient->runQuery(
            $wsBqClient->query('SELECT SESSION_USER() AS USER'),
        )->getIterator()->current();

        $credentialsArr = (array) json_decode($createResponse->getWorkspaceUserName(), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame($credentialsArr['client_email'], $result['USER']);
    }
}
