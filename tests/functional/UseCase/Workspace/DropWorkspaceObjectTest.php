<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Keboola\StorageDriver\BigQuery\Handler\Workspace\DropObject\DropWorkspaceObjectHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceObjectCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Throwable;

class DropWorkspaceObjectTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];
        $this->projectResponse = $this->projects[0][1];
    }

    public function testCreateDropWorkspace(): void
    {
        // CREATE
        [
            $credentials,
            $response,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse, $this->projects[0][2]);
        $this->assertInstanceOf(GenericBackendCredentials::class, $credentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $credentials);

        // create tables
        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE TABLE %s.`testTable` (`id` INTEGER);',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));
        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE TABLE %s.`testTable2` (`id` INTEGER);',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        // create view
        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE VIEW %s.`testView` AS '
            . 'SELECT `id` FROM %s.`testTable`;',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName()),
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName())
        )));

        // DROP with BAD TABLE NAME
        $handler = new DropWorkspaceObjectHandler($this->clientManager);
        $command = (new DropWorkspaceObjectCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName())
            ->setObjectNameToDrop('objectNotExists');

        try {
            $handler(
                $credentials,
                $command,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('Should fail');
        } catch (Throwable $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertStringContainsString(
                sprintf(
                    '%s.%s',
                    $response->getWorkspaceObjectName(),
                    'objectNotExists'
                ),
                $e->getMessage()
            );
        }

        // DROP with BAD TABLE NAME with IGNORE
        $handler = new DropWorkspaceObjectHandler($this->clientManager);
        $command = (new DropWorkspaceObjectCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName())
            ->setObjectNameToDrop('objectNotExists')
            ->setIgnoreIfNotExists(true);

        $dropResponse = $handler(
            $credentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertNull($dropResponse);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $credentials);
        $this->assertTrue($this->isTableExists($bqClient, $response->getWorkspaceObjectName(), 'testTable'));
        $this->assertTrue($this->isTableExists($bqClient, $response->getWorkspaceObjectName(), 'testTable2'));

        // DROP table
        $handler = new DropWorkspaceObjectHandler($this->clientManager);
        $command = (new DropWorkspaceObjectCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName())
            ->setObjectNameToDrop('testTable2');

        $dropResponse = $handler(
            $credentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertNull($dropResponse);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $credentials);
        $this->assertTrue($this->isTableExists($bqClient, $response->getWorkspaceObjectName(), 'testTable'));
        $this->assertFalse($this->isTableExists($bqClient, $response->getWorkspaceObjectName(), 'testTable2'));

        // DROP table used in view
        $handler = new DropWorkspaceObjectHandler($this->clientManager);
        $command = (new DropWorkspaceObjectCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName())
            ->setObjectNameToDrop('testTable');

        $dropResponse = $handler(
            $credentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertNull($dropResponse);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $credentials);
        $this->assertFalse($this->isTableExists($bqClient, $response->getWorkspaceObjectName(), 'testTable'));
        $this->assertFalse($this->isTableExists($bqClient, $response->getWorkspaceObjectName(), 'testTable2'));
    }
}
