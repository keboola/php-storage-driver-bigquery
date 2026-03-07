<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Keboola\StorageDriver\BigQuery\Handler\Workspace\Clear\ClearWorkspaceHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\ClearWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Throwable;

class ClearWorkspaceTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];
        $this->projectResponse = $this->projects[0][1];
    }

    public function testClearWorkspace(): void
    {
        // CREATE
        [
            $credentials,
            $response,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);
        $this->assertInstanceOf(GenericBackendCredentials::class, $credentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $credentials);

        // create tables
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'CREATE TABLE %s.`testTable` (`id` INTEGER);',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName()),
        )));
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'CREATE TABLE %s.`testTable2` (`id` INTEGER);',
            BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName()),
        )));

        // CLEAR with BAD OBJECT NAME
        $handler = new ClearWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ClearWorkspaceCommand())
            ->setWorkspaceObjectName('objectNotExists');

        try {
            $handler(
                $this->projectCredentials,
                $command,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('Should fail');
        } catch (Throwable $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertStringContainsString(
                'Not found: ',
                $e->getMessage(),
            );
        }

        // CLEAR with BAD OBJECT NAME and IGNORE ERRORS
        $handler = new ClearWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ClearWorkspaceCommand())
            ->setWorkspaceObjectName('objectNotExists')
            ->setIgnoreErrors(true);

        $clearResponse = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertNull($clearResponse);

        $projectBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $iamService = $this->clientManager->getIamClient($this->projectCredentials);
        $this->assertTrue($this->isTableExists($projectBqClient, $response->getWorkspaceObjectName(), 'testTable'));
        $this->assertTrue($this->isTableExists($projectBqClient, $response->getWorkspaceObjectName(), 'testTable2'));

        $this->assertTrue($this->isDatabaseExists($projectBqClient, $response->getWorkspaceObjectName()));
        $this->assertTrue($this->isUserExists($iamService, $response->getWorkspaceUserName()));

        // CLEAR but preserve testTable2
        $handler = new ClearWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ClearWorkspaceCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName())
            ->setObjectsToPreserve(ProtobufHelper::arrayToRepeatedString(['testTable2']));
        $clearResponse = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertNull($clearResponse);

        $projectBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $this->assertFalse($this->isTableExists($projectBqClient, $response->getWorkspaceObjectName(), 'testTable'));
        $this->assertTrue($this->isTableExists($projectBqClient, $response->getWorkspaceObjectName(), 'testTable2'));

        // CLEAR
        $handler = new ClearWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ClearWorkspaceCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());

        $clearResponse = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertNull($clearResponse);

        $projectBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $iamService = $this->clientManager->getIamClient($this->projectCredentials);
        $this->assertFalse($this->isTableExists($projectBqClient, $response->getWorkspaceObjectName(), 'testTable'));
        $this->assertFalse($this->isTableExists($projectBqClient, $response->getWorkspaceObjectName(), 'testTable2'));

        $this->assertTrue($this->isDatabaseExists($projectBqClient, $response->getWorkspaceObjectName()));
        $this->assertTrue($this->isUserExists($iamService, $response->getWorkspaceUserName()));
    }

    /**
     * SUPPORT-15365: ClearWorkspace must delete all tables even when the BigQuery
     * tables.list API paginates (default page size ~50). A previous bug caused the
     * iterator to skip the first page after an existence-check call to current().
     */
    public function testClearWorkspaceWithPaginatedTables(): void
    {
        $tableCount = 55;

        [
            $credentials,
            $response,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);
        $this->assertInstanceOf(GenericBackendCredentials::class, $credentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $credentials);
        $datasetName = BigqueryQuote::quoteSingleIdentifier($response->getWorkspaceObjectName());

        // Create 55 tables to exceed the default page size (~50) of tables.list API
        for ($i = 1; $i <= $tableCount; $i++) {
            $tableName = sprintf('test_table_%02d', $i);
            $wsBqClient->runQuery($wsBqClient->query(sprintf(
                'CREATE TABLE %s.`%s` (`id` INTEGER);',
                $datasetName,
                $tableName,
            )));
        }

        // Verify all tables exist
        $projectBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $tablesBeforeClear = iterator_to_array(
            $projectBqClient->dataset($response->getWorkspaceObjectName())->tables(),
        );
        $this->assertCount($tableCount, $tablesBeforeClear);

        // CLEAR - this must delete all 55 tables, not just those on page 2+
        $handler = new ClearWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ClearWorkspaceCommand())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());

        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify ALL tables are gone
        $tablesAfterClear = iterator_to_array(
            $projectBqClient->dataset($response->getWorkspaceObjectName())->tables(),
        );
        $this->assertCount(0, $tablesAfterClear, sprintf(
            'Expected 0 tables after clear, but found %d. '
            . 'This indicates the clear operation skipped some tables due to pagination.',
            count($tablesAfterClear),
        ));
    }
}
