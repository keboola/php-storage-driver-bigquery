<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Cloud\BigQuery\Dataset;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop\DropBucketHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Throwable;

class CreateDropBucketTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];
        $this->projectResponse = $this->projects[0][1];
    }

    public function testCreateDropBucket(): void
    {
        $response = $this->createTestBucket($this->projectCredentials, $this->projects[0][2]);

        $handler = new DropBucketHandler($this->clientManager);
        $command = (new DropBucketCommand())
            ->setBucketObjectName($response->getCreateBucketObjectName());

        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $bigQueryClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $dataset = $bigQueryClient->dataset($response->getCreateBucketObjectName());
        $this->assertFalse($dataset->exists());
    }

    public function testCreateBucketInBranch(): void
    {
        $response = $this->createTestBucket($this->projectCredentials, $this->projects[0][2], '123');

        $this->assertInstanceOf(CreateBucketResponse::class, $response);

        $bigQueryClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        $dataset = $bigQueryClient->dataset($response->getCreateBucketObjectName());

        $bucketInfo = $dataset->info();
        $this->assertArrayNotHasKey('defaultTableExpirationMs', $bucketInfo);
        $this->assertInstanceOf(Dataset::class, $dataset);
        $this->assertStringStartsWith('123_', $response->getCreateBucketObjectName());
        $this->assertEquals($response->getCreateBucketObjectName(), $dataset->identity()['datasetId']);
        $this->assertTrue($dataset->exists());
    }

    public function testCreateDropCascadeBucket(): void
    {
        $bucket = $this->createTestBucket($this->projectCredentials, $this->projects[0][2]);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $createdDataset = $bqClient->dataset($bucket->getCreateBucketObjectName());
        $tableName = $this->getTestHash() . '_Test_table';
        $createdDataset->createTable($tableName);

        $handler = new DropBucketHandler($this->clientManager);
        $command = (new DropBucketCommand())
            ->setBucketObjectName($bucket->getCreateBucketObjectName());

        try {
            $handler(
                $this->projectCredentials,
                $command,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('Should fail as bucket database contains table');
        } catch (Throwable $e) {
            /** @var array<string, array<string, string>>|false $message */
            $message = json_decode($e->getMessage(), true, 512, JSON_THROW_ON_ERROR);
            assert($message !== false);
            $this->assertStringContainsString(
                $bucket->getCreateBucketObjectName() . ' is still in use',
                $message['error']['message'],
            );
        }

        $dataset = $bqClient->dataset($bucket->getCreateBucketObjectName());
        $table = $dataset->table($tableName);
        $this->assertTrue($table->exists());

        $command->setIsCascade(true);
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $dataset = $bqClient->dataset($bucket->getCreateBucketObjectName());
        $table = $dataset->table($tableName);
        $this->assertFalse($table->exists());
    }
}
