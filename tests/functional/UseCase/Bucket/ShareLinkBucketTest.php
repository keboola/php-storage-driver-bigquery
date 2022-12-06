<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\ApiCore\ApiException;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnLink\UnLinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnShare\UnShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\LinkedBucketResponse;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketResponse;
use Keboola\StorageDriver\Command\Bucket\UnlinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\UnshareBucketCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

class ShareLinkBucketTest extends BaseCase
{
    protected GenericBackendCredentials $sourceProjectCredentials;
    protected CreateProjectResponse $sourceProjectResponse;

    protected GenericBackendCredentials $targetProjectCredentials;
    protected CreateProjectResponse $targetProjectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$credentials1, $response1] = $this->createTestProject();
        $this->projectSuffix = '-s';
        [$credentials2, $response2] = $this->createTestProject();

        // project1 shares bucket
        $this->sourceProjectCredentials = $credentials1;
        $this->sourceProjectResponse = $response1;

        // project2 checks the access
        $this->targetProjectCredentials = $credentials2;
        $this->targetProjectResponse = $response2;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testShareAndLinkBucket(): void
    {
        $bucketResponse = $this->createTestBucket($this->sourceProjectCredentials);

        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();

        $sourceBqClient = $this->clientManager->getBigQueryClient($this->sourceProjectCredentials);
        $linkedBucketSchemaName = $bucketDatabaseName . '_LINKED';

        $handler = new CreateTableHandler($this->clientManager);
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columns = new RepeatedField(GPBType::MESSAGE, CreateTableCommand\TableColumn::class);
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('ID')
            ->setType(Bigquery::TYPE_INTEGER);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName('TESTTABLE_BEFORE')
            ->setColumns($columns);
        $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );
        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.`TESTTABLE_BEFORE` (`ID`) VALUES (1)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName)
        )));

        $targetProjectBqClient = $this->clientManager->getBigQueryClient($this->targetProjectCredentials);

//      check that the Project2 cannot access the table yet
        $dataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertFalse($dataset->exists());

        $publicPart = (array) json_decode(
            $this->sourceProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $publicPart['project_id'];
        // share the bucket
        $handler = new ShareBucketHandler($this->clientManager);
        $command = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $result = $handler(
            $this->getCredentials(),
            $command,
            []
        );

        assert($result instanceof ShareBucketResponse);
        $listing = $result->getBucketShareRoleName();

        $publicPart = (array) json_decode(
            $this->targetProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR
        );
        /** @var string $targetProjectId */
        $targetProjectId = $publicPart['project_id'];
        // link the bucket
        $handler = new LinkBucketHandler($this->clientManager);
        $command = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($targetProjectId)
            ->setTargetBucketId($linkedBucketSchemaName)
            ->setSourceShareRoleName($listing); // listing

        // it is sourceProject who does the grants -> that's why the sourceProjectCredentials
        $result = $handler(
            $this->getCredentials(),
            $command,
            []
        );

        assert($result instanceof LinkedBucketResponse);
        $linkedBucketSchemaName = $result->getLinkedBucketObjectName();

        $handler = new CreateTableHandler($this->clientManager);
        $columns = new RepeatedField(GPBType::MESSAGE, CreateTableCommand\TableColumn::class);
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('ID')
            ->setType(Bigquery::TYPE_INTEGER);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName('TESTTABLE_AFTER')
            ->setColumns($columns);
        $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );
        // check that there is no need to re-share or whatever
        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.`TESTTABLE_AFTER` (`ID`) VALUES (1)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName)
        )));

        $targetDataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());
        $testTableBefore = $targetDataset->table('TESTTABLE_BEFORE');
        $this->assertTrue($testTableBefore->exists());
        $dataBefore = iterator_to_array($testTableBefore->rows());

        $testTableAfter = $targetDataset->table('TESTTABLE_AFTER');
        $this->assertTrue($testTableAfter->exists());
        $dataAfter = iterator_to_array($testTableAfter->rows());

        $this->assertEquals([['ID' => '1']], $dataAfter);
        $this->assertEquals($dataBefore, $dataAfter);

        // unlink and check that target project cannot access it anymore
        $unlinkHandler = new UnLinkBucketHandler($this->clientManager);
        $command = (new UnLinkBucketCommand())
            ->setBucketObjectName($linkedBucketSchemaName);

        $unlinkHandler(
            $this->targetProjectCredentials,
            $command,
            []
        );

        // check that the Project2 cannot access the table anymore
        $targetDataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertFalse($targetDataset->exists());
    }

    public function testShareUnshare(): void
    {
        $bucketResponse = $this->createTestBucket($this->sourceProjectCredentials);

        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $publicPart = (array) json_decode(
            $this->sourceProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $publicPart['project_id'];
        $handler = new ShareBucketHandler($this->clientManager);
        $command = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $handler(
            $this->getCredentials(),
            $command,
            []
        );

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($this->getCredentials());

        $formattedName = $analyticHubClient->listingName(
            $sourceProjectId,
            GCPClientManager::DEFAULT_LOCATION,
            $this->sourceProjectResponse->getProjectReadOnlyRoleName(),
            $bucketDatabaseName
        );
        $listing = $analyticHubClient->getListing($formattedName);
        $this->assertNotNull($listing->getName());

        $handler = new UnShareBucketHandler($this->clientManager);
        $command = (new UnShareBucketCommand())
            ->setBucketShareRoleName($listing->getName());

        $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );

        try {
            $analyticHubClient->getListing($formattedName);
            $this->fail('Should fail!');
        } catch (ApiException $e) {
            $this->assertSame('NOT_FOUND', $e->getStatus());
        }
    }

    public function testShareUnshareLinkedBucket(): void
    {
        $bucketResponse = $this->createTestBucket($this->sourceProjectCredentials);

        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();

        $publicPart = (array) json_decode(
            $this->sourceProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $publicPart['project_id'];
        $handler = new ShareBucketHandler($this->clientManager);
        $command = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $handler(
            $this->getCredentials(),
            $command,
            []
        );

        $sourceBqClient = $this->clientManager->getBigQueryClient($this->sourceProjectCredentials);

        $handler = new CreateTableHandler($this->clientManager);
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columns = new RepeatedField(GPBType::MESSAGE, CreateTableCommand\TableColumn::class);
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('ID')
            ->setType(Bigquery::TYPE_INTEGER);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName('TESTTABLE_AFTER')
            ->setColumns($columns);
        $handler(
            $this->sourceProjectCredentials,
            $command,
            []
        );
        // check that there is no need to re-share or whatever
        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.`TESTTABLE_AFTER` (`ID`) VALUES (1)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName)
        )));

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($this->getCredentials());

        $formattedName = $analyticHubClient->listingName(
            $sourceProjectId,
            GCPClientManager::DEFAULT_LOCATION,
            $this->sourceProjectResponse->getProjectReadOnlyRoleName(),
            $bucketDatabaseName
        );
        $listing = $analyticHubClient->getListing($formattedName);
        $this->assertNotNull($listing->getName());

        $publicPart = (array) json_decode(
            $this->targetProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR
        );
        /** @var string $targetProjectId */
        $targetProjectId = $publicPart['project_id'];
        $linkedBucketSchemaName = $bucketDatabaseName . '_LINKED';

        $handler = new LinkBucketHandler($this->clientManager);
        $command = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($targetProjectId)
            ->setTargetBucketId($linkedBucketSchemaName)
            ->setSourceShareRoleName($listing->getName()); // listing

        $handler(
            $this->getCredentials(),
            $command,
            []
        );

        $handler = new UnShareBucketHandler($this->clientManager);
        $command = (new UnShareBucketCommand())
            ->setBucketShareRoleName($listing->getName());

        $handler(
            $this->getCredentials(),
            $command,
            []
        );

        //@todo drop lisnting do not drop the linked dataset or fail, we can list all
        // linked datasets and drop it but is not an atomic operation
    }


    public function tmpCreateShareRoleNameForBucket(
        string $projectId,
        string $bucketId
    ): string {
        return strtoupper(sprintf(
            '%s_%s_SHARE',
            str_replace('-', '_', $projectId),
            $bucketId
        ));
    }
}
