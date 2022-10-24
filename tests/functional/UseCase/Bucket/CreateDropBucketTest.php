<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Cloud\BigQuery\Dataset;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop\DropBucketHandle;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
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
        $this->cleanTestProject();
        [$credentials, $response] = $this->createTestProject();
        $this->projectCredentials = $credentials;
        $this->projectResponse = $response;
    }

    public function testCreateDropBucket(): void
    {
        $response = $this->createTestBucket($this->projectCredentials);

        $handler = new DropBucketHandle($this->clientManager);
        $command = (new DropBucketCommand())
            ->setBucketObjectName($response->getCreateBucketObjectName());

        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $bigQueryClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $dataset = $bigQueryClient->dataset($response->getCreateBucketObjectName());
        $this->assertFalse($dataset->exists());
    }

    public function testCreateDropCascadeBucket(): void
    {
        $bucket = $this->createTestBucket($this->projectCredentials);

        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $createdDataset = $bqClient->dataset($bucket->getCreateBucketObjectName());
        $tableName = md5($this->getName()) . '_Test_table';
        $createdDataset->createTable($tableName);

        $handler = new DropBucketHandle($this->clientManager);
        $command = (new DropBucketCommand())
            ->setBucketObjectName($bucket->getCreateBucketObjectName());

        try {
            $handler(
                $this->projectCredentials,
                $command,
                []
            );
            $this->fail('Should fail as bucket database contains table');
        } catch (Throwable $e) {
            /** @var array<string, array<string, string>>|false $message */
            $message = json_decode($e->getMessage(), true, 512, JSON_THROW_ON_ERROR);
            assert($message !== false);
            $this->assertStringContainsString(
                $bucket->getCreateBucketObjectName() . ' is still in use',
                $message['error']['message']
            );
        }

        $dataset = $bqClient->dataset($bucket->getCreateBucketObjectName());
        $table = $dataset->table($tableName);
        $this->assertTrue($table->exists());

        // ignore errors should not fail but database is not removed
        $command->setIgnoreErrors(true);
        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $dataset = $bqClient->dataset($bucket->getCreateBucketObjectName());
        $table = $dataset->table($tableName);
        $this->assertTrue($table->exists());

        // should not fail and database will be deleted
        $command->setIgnoreErrors(false);
        $command->setIsCascade(true);
        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $dataset = $bqClient->dataset($bucket->getCreateBucketObjectName());
        $table = $dataset->table($tableName);
        $this->assertFalse($table->exists());
    }


}
