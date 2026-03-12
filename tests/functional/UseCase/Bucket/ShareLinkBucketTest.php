<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\ApiCore\ApiException;
use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnLink\UnLinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnShare\UnShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\AddColumnHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\DropColumnHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateViewHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\LinkedBucketResponse;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketResponse;
use Keboola\StorageDriver\Command\Bucket\UnlinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\UnshareBucketCommand;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\AddColumnCommand;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\CreateViewCommand;
use Keboola\StorageDriver\Command\Table\DropColumnCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Throwable;

class ShareLinkBucketTest extends BaseCase
{
    private const TESTTABLE_BEFORE_NAME = 'TESTTABLE_BEFORE';
    private const TESTTABLE_AFTER_NAME = 'TESTTABLE_AFTER';

    protected GenericBackendCredentials $sourceProjectCredentials;

    protected CreateProjectResponse $sourceProjectResponse;

    protected GenericBackendCredentials $targetProjectCredentials;

    protected CreateProjectResponse $targetProjectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        // project1 shares bucket
        $this->sourceProjectCredentials = $this->projects[0][0];
        $this->sourceProjectResponse = $this->projects[0][1];

        // project2 checks the access
        $this->targetProjectCredentials = $this->projects[1][0];
        $this->targetProjectResponse = $this->projects[1][1];
    }

    public function testShareAndLinkBucket(): void
    {
        $bucketResponse = $this->createTestBucket($this->sourceProjectCredentials);

        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();

        $sourceBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->sourceProjectCredentials);
        $linkedBucketSchemaName = $bucketDatabaseName . '_LINKED';

        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('ID')
            ->setType(Bigquery::TYPE_INTEGER);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName(self::TESTTABLE_BEFORE_NAME)
            ->setColumns($columns);
        $handler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`) VALUES (1)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier(self::TESTTABLE_BEFORE_NAME),
        )));

        $targetProjectBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );

//      check that the Project2 cannot access the table yet
        $dataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertFalse($dataset->exists());

        $publicPart = (array) json_decode(
            $this->sourceProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $publicPart['project_id'];
        // share the bucket
        $handler = new ShareBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceBucketId($this->getTestHash())
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $meta = new Any();
        $meta->pack((new ShareBucketCommand\ShareBucketBigqueryCommandMeta()));
        $command->setMeta($meta);
        $result = $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(ShareBucketResponse::class, $result);
        $listing = $result->getBucketShareRoleName();
        $this->assertNotEmpty($listing);
        $publicPart = (array) json_decode(
            $this->targetProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $targetProjectId */
        $targetProjectId = $publicPart['project_id'];
        // link the bucket
        $handler = new LinkBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($targetProjectId)
            ->setTargetBucketId($linkedBucketSchemaName)
            ->setSourceShareRoleName($listing); // listing

        $meta = new Any();
        $meta->pack(new LinkBucketCommand\LinkBucketBigqueryMeta());
        $command->setMeta($meta);
        // root credentials and creating grants
        $result = $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(LinkedBucketResponse::class, $result);
        $linkedBucketSchemaName = $result->getLinkedBucketObjectName();
        $this->assertNotEmpty($linkedBucketSchemaName);
        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared())
            ->setName('ID')
            ->setType(Bigquery::TYPE_INTEGER);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName(self::TESTTABLE_AFTER_NAME)
            ->setColumns($columns);
        $handler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        // check that there is no need to re-share or whatever
        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`) VALUES (1)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier(self::TESTTABLE_AFTER_NAME),
        )));

        $targetDataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());
        $testTableBefore = $targetDataset->table(self::TESTTABLE_BEFORE_NAME);
        $this->assertTrue($testTableBefore->exists());
        $dataBefore = iterator_to_array($testTableBefore->rows());

        $testTableAfter = $targetDataset->table(self::TESTTABLE_AFTER_NAME);
        $this->assertTrue($testTableAfter->exists());
        $dataAfter = iterator_to_array($testTableAfter->rows());

        $this->assertEquals([['ID' => '1']], $dataAfter);
        $this->assertEquals($dataBefore, $dataAfter);

        // unlink and check that target project cannot access it anymore
        $unlinkHandler = new UnLinkBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new UnLinkBucketCommand())
            ->setBucketObjectName($linkedBucketSchemaName);

        $unlinkHandler(
            $this->targetProjectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
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
            JSON_THROW_ON_ERROR,
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $publicPart['project_id'];
        $handler = new ShareBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceBucketId($this->getTestHash())
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $meta = new Any();
        $meta->pack((new ShareBucketCommand\ShareBucketBigqueryCommandMeta()));
        $command->setMeta($meta);
        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($this->getCredentials());

        $formattedName = $analyticHubClient::listingName(
            $sourceProjectId,
            BaseCase::DEFAULT_LOCATION,
            $this->sourceProjectResponse->getProjectReadOnlyRoleName(),
            $this->getTestHash(),
        );
        $listing = $analyticHubClient->getListing($formattedName);
        $this->assertNotNull($listing->getName());

        $handler = new UnShareBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new UnShareBucketCommand())
            ->setBucketShareRoleName($listing->getName());

        $handler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(),
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
            JSON_THROW_ON_ERROR,
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $publicPart['project_id'];
        $handler = new ShareBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceBucketId($this->getTestHash())
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $meta = new Any();
        $meta->pack(new ShareBucketCommand\ShareBucketBigqueryCommandMeta());
        $command->setMeta($meta);
        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $sourceBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->sourceProjectCredentials);

        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared())
            ->setName('ID')
            ->setType(Bigquery::TYPE_INTEGER);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName(self::TESTTABLE_AFTER_NAME)
            ->setColumns($columns);
        $handler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        // check that there is no need to re-share or whatever
        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`) VALUES (1)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier(self::TESTTABLE_AFTER_NAME),
        )));

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($this->getCredentials());

        $formattedName = $analyticHubClient::listingName(
            $sourceProjectId,
            BaseCase::DEFAULT_LOCATION,
            $this->sourceProjectResponse->getProjectReadOnlyRoleName(),
            $this->getTestHash(),
        );
        $listing = $analyticHubClient->getListing($formattedName);
        $this->assertNotNull($listing->getName());

        $publicPart = (array) json_decode(
            $this->targetProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $targetProjectId */
        $targetProjectId = $publicPart['project_id'];
        $linkedBucketSchemaName = $bucketDatabaseName . '_LINKED';

        $handler = new LinkBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($targetProjectId)
            ->setTargetBucketId($linkedBucketSchemaName)
            ->setSourceShareRoleName($listing->getName()); // listing

        $meta = new Any();
        $meta->pack(new LinkBucketCommand\LinkBucketBigqueryMeta());
        $command->setMeta($meta);
        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $targetProjectBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );
        $targetDataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());
        $testTableBefore = $targetDataset->table(self::TESTTABLE_AFTER_NAME);
        $this->assertTrue($testTableBefore->exists());

        $handler = new UnShareBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new UnShareBucketCommand())
            ->setBucketShareRoleName($listing->getName());

        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $targetProjectBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );
        $targetDataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());
        $testTableBefore = $targetDataset->table(self::TESTTABLE_AFTER_NAME);

        // after unshare the table is not available
        // in connection you can't just unshare a bucket that is lined up first so this is an edge case
        // handled in connection
        $this->expectException(BadRequestException::class);
        $testTableBefore->exists();
    }

    /**
     * Cross-bucket VIEW test for DMD-1104:
     * Table lives in bucket Ba, VIEW alias is created in bucket Bb pointing to Ba's table,
     * only Bb is shared via Analytics Hub. Verifies that the VIEW in Bb (referencing Ba)
     * is accessible through the linked dataset in the target project.
     *
     * Flow: Ba = table, Bb = VIEW(Ba.table) -> share Bb -> link Bb -> verify VIEW works
     */
    public function testViewInSourceDatasetAccessibleViaLinkedDataset(): void
    {
        // Parse source project ID (needed for stale cleanup and sharing)
        $sourcePublicPart = (array) json_decode(
            $this->sourceProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $sourcePublicPart['project_id'];

        $sourceBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->sourceProjectCredentials,
        );
        $targetBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );

        // --- Stale cleanup ---
        // Compute expected Bb dataset name for stale cleanup
        $bucketBbId = $this->getTestHash() . 'in.c-Test2';
        $bucketBbShareId = $this->getTestHash() . 'Bb'; // listing ID -- only [A-Za-z0-9_]
        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $expectedBbDatasetName = $nameGenerator->createObjectNameForBucketInProject($bucketBbId, null);
        $linkedBucketSchemaName = $expectedBbDatasetName . '_LINKED';

        // Cleanup stale linked dataset from previous runs
        try {
            $staleLinkedDataset = $targetBqClient->dataset($linkedBucketSchemaName);
            if ($staleLinkedDataset->exists()) {
                $staleLinkedDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        // Cleanup stale listing from previous runs
        $analyticHubClient = $this->clientManager->getAnalyticHubClient($this->sourceProjectCredentials);
        try {
            $staleListingName = AnalyticsHubServiceClient::listingName(
                $sourceProjectId,
                BaseCase::DEFAULT_LOCATION,
                $this->sourceProjectResponse->getProjectReadOnlyRoleName(),
                $bucketBbShareId,
            );
            $analyticHubClient->deleteListing($staleListingName);
        } catch (Throwable) {
            // ignore
        }

        // Cleanup stale Bb dataset from previous runs
        try {
            $staleBbDataset = $sourceBqClient->dataset($expectedBbDatasetName);
            if ($staleBbDataset->exists()) {
                $staleBbDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        // --- 1. Create two buckets in source project ---
        // Ba = table bucket (uses standard createTestBucket)
        $bucketBaResponse = $this->createTestBucket($this->sourceProjectCredentials);
        $bucketBaName = $bucketBaResponse->getCreateBucketObjectName();

        // Bb = VIEW bucket (manual creation with different bucketId)
        $bbHandler = new CreateBucketHandler($this->clientManager);
        $bbHandler->setInternalLogger($this->log);
        $bbCommand = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setBucketId($bucketBbId);
        $bbMeta = new Any();
        $bbMeta->pack(new CreateBucketCommand\CreateBucketBigqueryMeta());
        $bbCommand->setMeta($bbMeta);
        /** @var CreateBucketResponse $bbResponse */
        $bbResponse = $bbHandler(
            $this->sourceProjectCredentials,
            $bbCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $bbResponse);
        $bucketBbName = $bbResponse->getCreateBucketObjectName();

        // --- 2. Create table + data in Ba ---
        $tableHandler = new CreateTableHandler($this->clientManager);
        $tableHandler->setInternalLogger($this->log);
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketBaName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared())
            ->setName('ID')
            ->setType(Bigquery::TYPE_STRING);
        $columns[] = (new TableColumnShared())
            ->setName('NAME')
            ->setType(Bigquery::TYPE_STRING);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName(self::TESTTABLE_BEFORE_NAME)
            ->setColumns($columns);
        $tableHandler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`, `NAME`) VALUES (%s, %s), (%s, %s), (%s, %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketBaName),
            BigqueryQuote::quoteSingleIdentifier(self::TESTTABLE_BEFORE_NAME),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('alice'),
            BigqueryQuote::quote('2'),
            BigqueryQuote::quote('bob'),
            BigqueryQuote::quote('3'),
            BigqueryQuote::quote('charlie'),
        )));

        // --- 3. Create VIEW in Bb pointing to Ba's table (cross-dataset) ---
        $viewName = 'ALIAS_VIEW';
        $createViewHandler = new CreateViewHandler($this->clientManager);
        $createViewHandler->setInternalLogger($this->log);
        $createViewCommand = (new CreateViewCommand())
            ->setPath([$bucketBbName])
            ->setSourcePath([$bucketBaName])
            ->setViewName($viewName)
            ->setSourceTableName(self::TESTTABLE_BEFORE_NAME);
        $createViewHandler(
            $this->sourceProjectCredentials,
            $createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // --- 4. Create filtered VIEW in Bb pointing to Ba (raw SQL, handler doesn't support WHERE) ---
        $filteredViewName = 'FILTERED_ALIAS_VIEW';
        $filteredViewSql = sprintf(
            'CREATE VIEW %s.%s AS (SELECT * FROM %s.%s WHERE `ID` > %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketBbName),
            BigqueryQuote::quoteSingleIdentifier($filteredViewName),
            BigqueryQuote::quoteSingleIdentifier($bucketBaName),
            BigqueryQuote::quoteSingleIdentifier(self::TESTTABLE_BEFORE_NAME),
            BigqueryQuote::quote('1'),
        );
        $sourceBqClient->runQuery($sourceBqClient->query($filteredViewSql));

        // --- Grant Ba dataset access for filtered VIEW (created via raw SQL, not handler) ---
        // The ALIAS_VIEW authorized view grant is handled automatically by CreateViewHandler.
        // The FILTERED_ALIAS_VIEW was created via raw SQL, so we must grant it manually.
        $baDataset = $sourceBqClient->dataset($bucketBaName);
        $baInfo = $baDataset->reload();
        $currentAccess = $baInfo['access'] ?? [];
        $currentAccess[] = [
            'view' => [
                'projectId' => $sourceProjectId,
                'datasetId' => $bucketBbName,
                'tableId' => $filteredViewName,
            ],
        ];
        $baDataset->update(['access' => $currentAccess]);

        // Verify VIEWs work in source Bb dataset
        $sourceView = $sourceBqClient->dataset($bucketBbName)->table($viewName);
        $this->assertTrue($sourceView->exists(), 'VIEW should exist in Bb dataset');
        $sourceViewRows = $this->queryView($sourceBqClient, $bucketBbName, $viewName);
        $this->assertCount(3, $sourceViewRows, 'Source VIEW should return all 3 rows');

        $sourceFilteredView = $sourceBqClient->dataset($bucketBbName)->table($filteredViewName);
        $this->assertTrue($sourceFilteredView->exists(), 'Filtered VIEW should exist in Bb dataset');
        $sourceFilteredRows = $this->queryView($sourceBqClient, $bucketBbName, $filteredViewName);
        $this->assertCount(2, $sourceFilteredRows, 'Source filtered VIEW should return 2 rows');

        // --- 5. Share Bb (NOT Ba) via Analytics Hub ---
        $shareHandler = new ShareBucketHandler($this->clientManager);
        $shareHandler->setInternalLogger($this->log);
        $shareCommand = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketBbName)
            ->setSourceBucketId($bucketBbShareId)
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $meta = new Any();
        $meta->pack(new ShareBucketCommand\ShareBucketBigqueryCommandMeta());
        $shareCommand->setMeta($meta);
        $shareResult = $shareHandler(
            $this->getCredentials(),
            $shareCommand,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(ShareBucketResponse::class, $shareResult);
        $listing = $shareResult->getBucketShareRoleName();
        $this->assertNotEmpty($listing);

        // --- 6. Link Bb in target project ---
        $publicPart = (array) json_decode(
            $this->targetProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $targetProjectId */
        $targetProjectId = $publicPart['project_id'];

        $linkHandler = new LinkBucketHandler($this->clientManager);
        $linkHandler->setInternalLogger($this->log);
        $linkCommand = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($targetProjectId)
            ->setTargetBucketId($linkedBucketSchemaName)
            ->setSourceShareRoleName($listing);

        $linkMeta = new Any();
        $linkMeta->pack(new LinkBucketCommand\LinkBucketBigqueryMeta());
        $linkCommand->setMeta($linkMeta);
        $linkResult = $linkHandler(
            $this->getCredentials(),
            $linkCommand,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(LinkedBucketResponse::class, $linkResult);
        $linkedBucketSchemaName = $linkResult->getLinkedBucketObjectName();
        $this->assertNotEmpty($linkedBucketSchemaName);

        // --- 7. Verify linked dataset exists and VIEWs are accessible ---
        $targetBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );

        $targetDataset = $targetBqClient->dataset($linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());

        // VIEW is accessible via linked dataset
        $linkedView = $targetDataset->table($viewName);
        $this->assertTrue($linkedView->exists(), 'VIEW should be visible in linked dataset');
        $linkedViewRows = $this->queryView($targetBqClient, $linkedBucketSchemaName, $viewName);
        $this->assertCount(3, $linkedViewRows, 'VIEW via linked dataset should return all 3 rows');

        // Filtered VIEW is accessible via linked dataset
        $linkedFilteredView = $targetDataset->table($filteredViewName);
        $this->assertTrue($linkedFilteredView->exists(), 'Filtered VIEW should be visible in linked dataset');
        $linkedFilteredRows = $this->queryView($targetBqClient, $linkedBucketSchemaName, $filteredViewName);
        $this->assertCount(2, $linkedFilteredRows, 'Filtered VIEW via linked dataset should return 2 rows');

        // --- 8. Create workspace in target project and load VIEW via LoadTableToWorkspaceHandler ---
        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $workspaceDataset = $workspaceResponse->getWorkspaceObjectName();

        // --- 9. Load VIEW from linked dataset into workspace using FULL import ---
        $wsLoadDestTable = 'WS_LOADED_FROM_VIEW';
        $loadCmd = new LoadTableToWorkspaceCommand();

        $sourcePath = new RepeatedField(GPBType::STRING);
        $sourcePath[] = $linkedBucketSchemaName;
        $loadCmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($sourcePath)
                ->setTableName($viewName),
        );

        $destPath = new RepeatedField(GPBType::STRING);
        $destPath[] = $workspaceDataset;
        $loadCmd->setDestination(
            (new Table())
                ->setPath($destPath)
                ->setTableName($wsLoadDestTable),
        );
        $loadCmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL),
        );

        $loadHandler = new LoadTableToWorkspaceHandler($this->clientManager);
        $loadHandler->setInternalLogger($this->log);
        /** @var TableImportResponse $loadResponse */
        $loadResponse = $loadHandler(
            $this->targetProjectCredentials,
            $loadCmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertSame(3, $loadResponse->getImportedRowsCount());
        $wsLoadedRows = $this->queryView($targetBqClient, $workspaceDataset, $wsLoadDestTable);
        $this->assertCount(3, $wsLoadedRows, 'Workspace table loaded from VIEW should have 3 rows');

        // --- 10. Load filtered VIEW from linked dataset into workspace ---
        $wsLoadFilteredTable = 'WS_LOADED_FROM_FILTERED_VIEW';
        $loadFilteredCmd = new LoadTableToWorkspaceCommand();

        $loadFilteredCmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($sourcePath)
                ->setTableName($filteredViewName),
        );

        $filteredDestPath = new RepeatedField(GPBType::STRING);
        $filteredDestPath[] = $workspaceDataset;
        $loadFilteredCmd->setDestination(
            (new Table())
                ->setPath($filteredDestPath)
                ->setTableName($wsLoadFilteredTable),
        );
        $loadFilteredCmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL),
        );

        /** @var TableImportResponse $loadFilteredResponse */
        $loadFilteredResponse = $loadHandler(
            $this->targetProjectCredentials,
            $loadFilteredCmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertSame(2, $loadFilteredResponse->getImportedRowsCount());
        $wsFilteredRows = $this->queryView($targetBqClient, $workspaceDataset, $wsLoadFilteredTable);
        $this->assertCount(2, $wsFilteredRows, 'Workspace table loaded from filtered VIEW should have 2 rows');

        // --- 11. Verify workspace user can see loaded tables and read from them ---
        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $workspaceCredentials);

        $wsUserTable = $wsBqClient->dataset($workspaceDataset)->table($wsLoadDestTable);
        $this->assertTrue($wsUserTable->exists(), 'WS user should see table loaded from VIEW');
        $wsUserRows = $this->queryView($wsBqClient, $workspaceDataset, $wsLoadDestTable);
        $this->assertCount(3, $wsUserRows, 'WS user should read 3 rows from table loaded from VIEW');

        $wsUserFilteredTable = $wsBqClient->dataset($workspaceDataset)->table($wsLoadFilteredTable);
        $this->assertTrue($wsUserFilteredTable->exists(), 'WS user should see table loaded from filtered VIEW');
        $wsUserFilteredRows = $this->queryView($wsBqClient, $workspaceDataset, $wsLoadFilteredTable);
        $this->assertCount(2, $wsUserFilteredRows, 'WS user should read 2 rows from table loaded from filtered VIEW');

        // --- 12. Verify workspace user can read VIEWs in linked dataset directly ---
        $wsLinkedViewRows = $this->queryView($wsBqClient, $linkedBucketSchemaName, $viewName);
        $this->assertCount(3, $wsLinkedViewRows, 'WS user should read VIEW in linked dataset directly');

        $wsLinkedFilteredRows = $this->queryView($wsBqClient, $linkedBucketSchemaName, $filteredViewName);
        $this->assertCount(2, $wsLinkedFilteredRows, 'WS user should read filtered VIEW in linked dataset directly');

        // --- 13. Cleanup: unlink Bb -> unshare Bb ---
        $this->cleanupLinkedDataset(
            $linkedBucketSchemaName,
            $listing,
        );

        // Cleanup Bb dataset
        try {
            $bbDataset = $sourceBqClient->dataset($bucketBbName);
            if ($bbDataset->exists()) {
                $bbDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }
    }

    /**
     * Test that column changes on a source table are reflected through
     * a cross-dataset VIEW accessed via a linked dataset.
     *
     * BigQuery VIEWs with SELECT * resolve columns at query time, so adding
     * or dropping columns on the source table is automatically visible through
     * the VIEW without recreating it.
     */
    public function testSourceTableColumnChangesReflectedViaLinkedView(): void
    {
        // Parse source project ID
        $sourcePublicPart = (array) json_decode(
            $this->sourceProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $sourcePublicPart['project_id'];

        $sourceBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->sourceProjectCredentials,
        );
        $targetBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );

        // --- Stale cleanup ---
        $bucketBbId = $this->getTestHash() . 'in.c-TestColChange';
        $bucketBbShareId = $this->getTestHash() . 'BbCol';
        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $expectedBbDatasetName = $nameGenerator->createObjectNameForBucketInProject($bucketBbId, null);
        $linkedBucketSchemaName = $expectedBbDatasetName . '_LINKED';

        // Cleanup stale linked dataset
        try {
            $staleLinkedDataset = $targetBqClient->dataset($linkedBucketSchemaName);
            if ($staleLinkedDataset->exists()) {
                $staleLinkedDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        // Cleanup stale listing
        $analyticHubClient = $this->clientManager->getAnalyticHubClient($this->sourceProjectCredentials);
        try {
            $staleListingName = AnalyticsHubServiceClient::listingName(
                $sourceProjectId,
                BaseCase::DEFAULT_LOCATION,
                $this->sourceProjectResponse->getProjectReadOnlyRoleName(),
                $bucketBbShareId,
            );
            $analyticHubClient->deleteListing($staleListingName);
        } catch (Throwable) {
            // ignore
        }

        // Cleanup stale Bb dataset
        try {
            $staleBbDataset = $sourceBqClient->dataset($expectedBbDatasetName);
            if ($staleBbDataset->exists()) {
                $staleBbDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        // --- 1. Create two buckets in source project ---
        $bucketBaResponse = $this->createTestBucket($this->sourceProjectCredentials);
        $bucketBaName = $bucketBaResponse->getCreateBucketObjectName();

        $bbHandler = new CreateBucketHandler($this->clientManager);
        $bbHandler->setInternalLogger($this->log);
        $bbCommand = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setBucketId($bucketBbId);
        $bbMeta = new Any();
        $bbMeta->pack(new CreateBucketCommand\CreateBucketBigqueryMeta());
        $bbCommand->setMeta($bbMeta);
        /** @var CreateBucketResponse $bbResponse */
        $bbResponse = $bbHandler(
            $this->sourceProjectCredentials,
            $bbCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $bbResponse);
        $bucketBbName = $bbResponse->getCreateBucketObjectName();

        // --- 2. Create table with ID, NAME columns in Ba ---
        $tableName = self::TESTTABLE_BEFORE_NAME;
        $tableHandler = new CreateTableHandler($this->clientManager);
        $tableHandler->setInternalLogger($this->log);
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketBaName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared())
            ->setName('ID')
            ->setType(Bigquery::TYPE_STRING);
        $columns[] = (new TableColumnShared())
            ->setName('NAME')
            ->setType(Bigquery::TYPE_STRING);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns);
        $tableHandler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Insert 3 rows
        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`, `NAME`) VALUES (%s, %s), (%s, %s), (%s, %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketBaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('alice'),
            BigqueryQuote::quote('2'),
            BigqueryQuote::quote('bob'),
            BigqueryQuote::quote('3'),
            BigqueryQuote::quote('charlie'),
        )));

        // --- 3. Create SELECT * VIEW in Bb -> Ba ---
        $viewName = 'ALIAS_VIEW';
        $createViewHandler = new CreateViewHandler($this->clientManager);
        $createViewHandler->setInternalLogger($this->log);
        $createViewCommand = (new CreateViewCommand())
            ->setPath([$bucketBbName])
            ->setSourcePath([$bucketBaName])
            ->setViewName($viewName)
            ->setSourceTableName($tableName);
        $createViewHandler(
            $this->sourceProjectCredentials,
            $createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // --- 4. Create filtered VIEW in Bb (WHERE ID > '1') ---
        $filteredViewName = 'FILTERED_ALIAS_VIEW';
        $filteredViewSql = sprintf(
            'CREATE VIEW %s.%s AS (SELECT * FROM %s.%s WHERE `ID` > %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketBbName),
            BigqueryQuote::quoteSingleIdentifier($filteredViewName),
            BigqueryQuote::quoteSingleIdentifier($bucketBaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
            BigqueryQuote::quote('1'),
        );
        $sourceBqClient->runQuery($sourceBqClient->query($filteredViewSql));

        // Grant Ba dataset access for filtered VIEW (raw SQL view needs manual grant)
        $baDataset = $sourceBqClient->dataset($bucketBaName);
        $baInfo = $baDataset->reload();
        $currentAccess = $baInfo['access'] ?? [];
        $currentAccess[] = [
            'view' => [
                'projectId' => $sourceProjectId,
                'datasetId' => $bucketBbName,
                'tableId' => $filteredViewName,
            ],
        ];
        $baDataset->update(['access' => $currentAccess]);

        // --- 5. Share Bb via Analytics Hub ---
        $shareHandler = new ShareBucketHandler($this->clientManager);
        $shareHandler->setInternalLogger($this->log);
        $shareCommand = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketBbName)
            ->setSourceBucketId($bucketBbShareId)
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $meta = new Any();
        $meta->pack(new ShareBucketCommand\ShareBucketBigqueryCommandMeta());
        $shareCommand->setMeta($meta);
        $shareResult = $shareHandler(
            $this->getCredentials(),
            $shareCommand,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(ShareBucketResponse::class, $shareResult);
        $listing = $shareResult->getBucketShareRoleName();
        $this->assertNotEmpty($listing);

        // --- 6. Link Bb in target project ---
        $publicPart = (array) json_decode(
            $this->targetProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $targetProjectId */
        $targetProjectId = $publicPart['project_id'];

        $linkHandler = new LinkBucketHandler($this->clientManager);
        $linkHandler->setInternalLogger($this->log);
        $linkCommand = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($targetProjectId)
            ->setTargetBucketId($linkedBucketSchemaName)
            ->setSourceShareRoleName($listing);

        $linkMeta = new Any();
        $linkMeta->pack(new LinkBucketCommand\LinkBucketBigqueryMeta());
        $linkCommand->setMeta($linkMeta);
        $linkResult = $linkHandler(
            $this->getCredentials(),
            $linkCommand,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(LinkedBucketResponse::class, $linkResult);
        $linkedBucketSchemaName = $linkResult->getLinkedBucketObjectName();
        $this->assertNotEmpty($linkedBucketSchemaName);

        // --- 7. Verify initial state: both VIEWs work ---
        $targetBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );

        $targetDataset = $targetBqClient->dataset($linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());

        $linkedViewRows = $this->queryView($targetBqClient, $linkedBucketSchemaName, $viewName);
        $this->assertCount(3, $linkedViewRows, 'VIEW via linked dataset should return 3 rows initially');

        $linkedFilteredRows = $this->queryView($targetBqClient, $linkedBucketSchemaName, $filteredViewName);
        $this->assertCount(2, $linkedFilteredRows, 'Filtered VIEW via linked dataset should return 2 rows initially');

        // --- 7b. Create workspace in target project for RO role verification ---
        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $workspaceDataset = $workspaceResponse->getWorkspaceObjectName();
        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $workspaceCredentials);

        $loadHandler = new LoadTableToWorkspaceHandler($this->clientManager);
        $loadHandler->setInternalLogger($this->log);
        $sourcePath = new RepeatedField(GPBType::STRING);
        $sourcePath[] = $linkedBucketSchemaName;

        // Workspace user can read VIEWs in linked dataset directly (initial state)
        $wsLinkedViewRows = $this->queryView($wsBqClient, $linkedBucketSchemaName, $viewName);
        $this->assertCount(3, $wsLinkedViewRows, 'WS user should read 3 rows from VIEW initially');
        $wsLinkedFilteredRows = $this->queryView($wsBqClient, $linkedBucketSchemaName, $filteredViewName);
        $this->assertCount(2, $wsLinkedFilteredRows, 'WS user should read 2 rows from filtered VIEW initially');

        // --- 8. Add column AGE to source table ---
        $addColumnHandler = new AddColumnHandler($this->clientManager);
        $addColumnHandler->setInternalLogger($this->log);
        $addColumnCommand = (new AddColumnCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumnDefinition(
                (new TableColumnShared())
                    ->setName('AGE')
                    ->setType(Bigquery::TYPE_STRING)
                    ->setNullable(true),
            );
        $addColumnHandler(
            $this->sourceProjectCredentials,
            $addColumnCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Insert a new row with the new column
        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`, `NAME`, `AGE`) VALUES (%s, %s, %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketBaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
            BigqueryQuote::quote('4'),
            BigqueryQuote::quote('dave'),
            BigqueryQuote::quote('25'),
        )));

        // Verify VIEW via linked dataset: 4 rows, AGE column present
        $linkedViewRows = $this->queryView($targetBqClient, $linkedBucketSchemaName, $viewName);
        $this->assertCount(4, $linkedViewRows, 'VIEW should return 4 rows after adding column and row');
        $this->assertArrayHasKey('AGE', $linkedViewRows[0], 'New AGE column should be visible through VIEW');

        // Verify filtered VIEW: 3 rows (ID > '1' = bob, charlie, dave), AGE column present
        $linkedFilteredRows = $this->queryView($targetBqClient, $linkedBucketSchemaName, $filteredViewName);
        $this->assertCount(3, $linkedFilteredRows, 'Filtered VIEW should return 3 rows after adding column and row');
        $this->assertArrayHasKey('AGE', $linkedFilteredRows[0], 'New AGE column should be visible through filtered VIEW');

        // Verify workspace user (RO role) sees added column via direct query
        $wsLinkedViewRows = $this->queryView($wsBqClient, $linkedBucketSchemaName, $viewName);
        $this->assertCount(4, $wsLinkedViewRows, 'WS user should read 4 rows from VIEW after add column');
        $this->assertArrayHasKey('AGE', $wsLinkedViewRows[0], 'WS user should see AGE column in VIEW');

        $wsLinkedFilteredRows = $this->queryView($wsBqClient, $linkedBucketSchemaName, $filteredViewName);
        $this->assertCount(3, $wsLinkedFilteredRows, 'WS user should read 3 rows from filtered VIEW after add column');
        $this->assertArrayHasKey('AGE', $wsLinkedFilteredRows[0], 'WS user should see AGE column in filtered VIEW');

        // Recreate VIEW to refresh BigQuery metadata (frozen at creation time)
        $createViewHandler(
            $this->sourceProjectCredentials,
            $createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Load VIEW into workspace after add column
        $wsAddColTable = 'WS_VIEW_AFTER_ADD_COL';
        $loadCmd = new LoadTableToWorkspaceCommand();
        $loadCmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($sourcePath)
                ->setTableName($viewName),
        );
        $destPath = new RepeatedField(GPBType::STRING);
        $destPath[] = $workspaceDataset;
        $loadCmd->setDestination(
            (new Table())
                ->setPath($destPath)
                ->setTableName($wsAddColTable),
        );
        $loadCmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL),
        );
        /** @var TableImportResponse $loadResponse */
        $loadResponse = $loadHandler(
            $this->targetProjectCredentials,
            $loadCmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertSame(4, $loadResponse->getImportedRowsCount());
        $wsLoadedRows = $this->queryView($targetBqClient, $workspaceDataset, $wsAddColTable);
        $this->assertCount(4, $wsLoadedRows, 'WS loaded table should have 4 rows after add column');
        $this->assertArrayHasKey('AGE', $wsLoadedRows[0], 'WS loaded table should have AGE column');

        // --- 9. Drop NAME column from source table ---
        $dropColumnHandler = new DropColumnHandler($this->clientManager);
        $dropColumnHandler->setInternalLogger($this->log);
        $dropColumnCommand = (new DropColumnCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumnName('NAME');
        $dropColumnHandler(
            $this->sourceProjectCredentials,
            $dropColumnCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify VIEW via linked dataset: 4 rows, only ID and AGE columns
        $linkedViewRows = $this->queryView($targetBqClient, $linkedBucketSchemaName, $viewName);
        $this->assertCount(4, $linkedViewRows, 'VIEW should still return 4 rows after dropping column');
        $this->assertArrayHasKey('ID', $linkedViewRows[0], 'ID column should remain');
        $this->assertArrayHasKey('AGE', $linkedViewRows[0], 'AGE column should remain');
        $this->assertArrayNotHasKey('NAME', $linkedViewRows[0], 'Dropped NAME column should not be visible');

        // Verify filtered VIEW: 3 rows, only ID and AGE columns
        $linkedFilteredRows = $this->queryView($targetBqClient, $linkedBucketSchemaName, $filteredViewName);
        $this->assertCount(3, $linkedFilteredRows, 'Filtered VIEW should still return 3 rows after dropping column');
        $this->assertArrayHasKey('ID', $linkedFilteredRows[0], 'ID column should remain in filtered VIEW');
        $this->assertArrayHasKey('AGE', $linkedFilteredRows[0], 'AGE column should remain in filtered VIEW');
        $this->assertArrayNotHasKey('NAME', $linkedFilteredRows[0], 'Dropped NAME column should not be visible in filtered VIEW');

        // Verify workspace user (RO role) sees dropped column via direct query
        $wsLinkedViewRows = $this->queryView($wsBqClient, $linkedBucketSchemaName, $viewName);
        $this->assertCount(4, $wsLinkedViewRows, 'WS user should read 4 rows from VIEW after drop column');
        $this->assertArrayHasKey('ID', $wsLinkedViewRows[0], 'WS user should see ID in VIEW after drop');
        $this->assertArrayHasKey('AGE', $wsLinkedViewRows[0], 'WS user should see AGE in VIEW after drop');
        $this->assertArrayNotHasKey('NAME', $wsLinkedViewRows[0], 'WS user should not see NAME in VIEW after drop');

        $wsLinkedFilteredRows = $this->queryView($wsBqClient, $linkedBucketSchemaName, $filteredViewName);
        $this->assertCount(3, $wsLinkedFilteredRows, 'WS user should read 3 rows from filtered VIEW after drop column');
        $this->assertArrayNotHasKey('NAME', $wsLinkedFilteredRows[0], 'WS user should not see NAME in filtered VIEW after drop');

        // Recreate VIEW to refresh BigQuery metadata (frozen at creation time)
        $createViewHandler(
            $this->sourceProjectCredentials,
            $createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Load VIEW into workspace after drop column
        $wsDropColTable = 'WS_VIEW_AFTER_DROP_COL';
        $loadDropCmd = new LoadTableToWorkspaceCommand();
        $loadDropCmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($sourcePath)
                ->setTableName($viewName),
        );
        $destDropPath = new RepeatedField(GPBType::STRING);
        $destDropPath[] = $workspaceDataset;
        $loadDropCmd->setDestination(
            (new Table())
                ->setPath($destDropPath)
                ->setTableName($wsDropColTable),
        );
        $loadDropCmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL),
        );
        /** @var TableImportResponse $loadDropResponse */
        $loadDropResponse = $loadHandler(
            $this->targetProjectCredentials,
            $loadDropCmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertSame(4, $loadDropResponse->getImportedRowsCount());
        $wsDroppedRows = $this->queryView($targetBqClient, $workspaceDataset, $wsDropColTable);
        $this->assertCount(4, $wsDroppedRows, 'WS loaded table should have 4 rows after drop column');
        $this->assertArrayHasKey('ID', $wsDroppedRows[0]);
        $this->assertArrayHasKey('AGE', $wsDroppedRows[0]);
        $this->assertArrayNotHasKey('NAME', $wsDroppedRows[0], 'WS loaded table should not have NAME after drop');

        // --- 10. Cleanup ---
        $this->cleanupLinkedDataset(
            $linkedBucketSchemaName,
            $listing,
        );

        try {
            $bbDataset = $sourceBqClient->dataset($bucketBbName);
            if ($bbDataset->exists()) {
                $bbDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function queryView(
        BigQueryClient $bqClient,
        string $datasetName,
        string $viewName,
    ): array {
        $sql = sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($datasetName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
        );
        $queryResults = $bqClient->runQuery($bqClient->query($sql));

        /** @var list<array<string, mixed>> */
        return iterator_to_array($queryResults);
    }

    private function cleanupLinkedDataset(
        string $linkedBucketSchemaName,
        string $listing,
    ): void {
        $unlinkHandler = new UnLinkBucketHandler($this->clientManager);
        $unlinkHandler->setInternalLogger($this->log);
        $unlinkCommand = (new UnLinkBucketCommand())
            ->setBucketObjectName($linkedBucketSchemaName);

        $unlinkHandler(
            $this->targetProjectCredentials,
            $unlinkCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $unshareHandler = new UnShareBucketHandler($this->clientManager);
        $unshareHandler->setInternalLogger($this->log);
        $unshareCommand = (new UnShareBucketCommand())
            ->setBucketShareRoleName($listing);

        $unshareHandler(
            $this->sourceProjectCredentials,
            $unshareCommand,
            [],
            new RuntimeOptions(),
        );
    }
}
