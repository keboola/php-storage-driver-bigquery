<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\ExecuteQuery;

use Google\Cloud\BigQuery\QueryJobConfiguration;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\Handler\ExecuteQuery\ExecuteQueryHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\ExecuteQuery\ExecuteQueryCommand;
use Keboola\StorageDriver\Command\ExecuteQuery\ExecuteQueryResponse;
use Keboola\StorageDriver\Command\ExecuteQuery\ExecuteQueryResponse\Status;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Connection\Bigquery\QueryTagKey;

class QueryLabelsTest extends BaseCase
{
    private GenericBackendCredentials $projectCredentials;
    private string $workspaceName;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];

        // create workspace
        [
            ,
            $workspaceResponse,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projects[0][1]);

        $this->workspaceName = $workspaceResponse->getWorkspaceObjectName();
    }

    public function testQueryLabels(): void
    {
        // Create a simple query
        $query = 'SELECT 1 AS col1, "test" AS col2';
        $command = new ExecuteQueryCommand([
            'pathRestriction' => ProtobufHelper::arrayToRepeatedString([$this->workspaceName]),
        ]);
        $command->setQuery($query);

        $handler = new ExecuteQueryHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        // Get BigQuery client to check job metadata later
        $bqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->projectCredentials,
        );

        // Execute query with run_id and branch_id labels
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            (new RuntimeOptions(['runId' => $this->testRunId]))
                ->setQueryTags([QueryTagKey::BRANCH_ID->value => '123-branch']),
        );

        $this->assertInstanceOf(ExecuteQueryResponse::class, $response);
        $this->assertEquals(Status::Success, $response->getStatus());
        $this->assertNotNull($response->getData());
        $this->assertStringContainsString('successfully', $response->getMessage());

        // Get the most recent job
        $jobs = iterator_to_array($bqClient->jobs(['maxResults' => 1]));
        $this->assertNotEmpty($jobs, 'No jobs found');

        /** @var \Google\Cloud\BigQuery\Job $job */
        $job = $jobs[0];

        // Get job details using job ID
        $jobId = $job->id();
        $jobDetails = $bqClient->job($jobId);
        $info = $jobDetails->info();

        // Verify the labels are set correctly
        $this->assertArrayHasKey('configuration', $info);
        $this->assertArrayHasKey('labels', $info['configuration']);
        $labels = $info['configuration']['labels'];
        $this->assertNotNull($labels, 'No labels found on the query job');

        $this->assertArrayHasKey('run_id', $labels);
        $this->assertEquals($this->testRunId, $labels['run_id']);

        $this->assertArrayHasKey('branch_id', $labels);
        $this->assertEquals('123-branch', $labels['branch_id']);
    }
}
