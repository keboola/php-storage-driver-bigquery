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
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnLink\UnLinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnShare\UnShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\LinkedBucketResponse;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketResponse;
use Keboola\StorageDriver\Command\Bucket\UnlinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\UnshareBucketCommand;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
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
     * PoC for DMD-1104: Can a VIEW created in source dataset be accessed via Analytics Hub linked dataset?
     *
     * Flow: CREATE VIEW in source dataset -> share dataset via Analytics Hub -> link in target project
     * -> verify VIEW is accessible and returns correct data through the linked dataset.
     */
    public function testViewInSourceDatasetAccessibleViaLinkedDataset(): void
    {
        // 1. Create source bucket + table with data
        $bucketResponse = $this->createTestBucket($this->sourceProjectCredentials);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $linkedBucketSchemaName = $bucketDatabaseName . '_LINKED';

        $sourceBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->sourceProjectCredentials,
        );

        // Cleanup stale linked dataset from previous runs
        $targetBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );
        try {
            $staleLinkedDataset = $targetBqClient->dataset($linkedBucketSchemaName);
            if ($staleLinkedDataset->exists()) {
                $staleLinkedDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        // Cleanup stale listing from previous runs
        $sourcePublicPart = (array) json_decode(
            $this->sourceProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $sourcePublicPart['project_id'];
        $analyticHubClient = $this->clientManager->getAnalyticHubClient($this->sourceProjectCredentials);
        try {
            $staleListingName = AnalyticsHubServiceClient::listingName(
                $sourceProjectId,
                BaseCase::DEFAULT_LOCATION,
                $this->sourceProjectResponse->getProjectReadOnlyRoleName(),
                $this->getTestHash(),
            );
            $analyticHubClient->deleteListing($staleListingName);
        } catch (Throwable) {
            // ignore
        }

        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
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
        $handler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`, `NAME`) VALUES (%s, %s), (%s, %s), (%s, %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier(self::TESTTABLE_BEFORE_NAME),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('alice'),
            BigqueryQuote::quote('2'),
            BigqueryQuote::quote('bob'),
            BigqueryQuote::quote('3'),
            BigqueryQuote::quote('charlie'),
        )));

        // 2. Create VIEW in source dataset (simulates alias materialized as VIEW)
        $viewName = 'ALIAS_VIEW';
        $viewSql = sprintf(
            'CREATE VIEW %s.%s AS (SELECT * FROM %s.%s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier(self::TESTTABLE_BEFORE_NAME),
        );
        $sourceBqClient->runQuery($sourceBqClient->query($viewSql));

        // Create filtered VIEW (simulates filtered alias with WHERE clause)
        $filteredViewName = 'FILTERED_ALIAS_VIEW';
        $filteredViewSql = sprintf(
            'CREATE VIEW %s.%s AS (SELECT * FROM %s.%s WHERE `ID` > %s)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($filteredViewName),
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier(self::TESTTABLE_BEFORE_NAME),
            BigqueryQuote::quote('1'),
        );
        $sourceBqClient->runQuery($sourceBqClient->query($filteredViewSql));

        // Verify VIEWs work in source dataset
        $sourceView = $sourceBqClient->dataset($bucketDatabaseName)->table($viewName);
        $this->assertTrue($sourceView->exists(), 'VIEW should exist in source dataset');
        $sourceViewRows = $this->queryView($sourceBqClient, $bucketDatabaseName, $viewName);
        $this->assertCount(3, $sourceViewRows, 'Source VIEW should return all 3 rows');

        $sourceFilteredView = $sourceBqClient->dataset($bucketDatabaseName)->table($filteredViewName);
        $this->assertTrue($sourceFilteredView->exists(), 'Filtered VIEW should exist in source dataset');
        $sourceFilteredRows = $this->queryView($sourceBqClient, $bucketDatabaseName, $filteredViewName);
        $this->assertCount(2, $sourceFilteredRows, 'Source filtered VIEW should return 2 rows');

        // 3. Share bucket via Analytics Hub (sourceProjectId already parsed in cleanup above)
        $shareHandler = new ShareBucketHandler($this->clientManager);
        $shareHandler->setInternalLogger($this->log);
        $shareCommand = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceBucketId($this->getTestHash())
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

        // 4. Link bucket to target project
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

        // 5. Verify linked dataset exists
        $targetBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );

        $targetDataset = $targetBqClient->dataset($linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());

        // Verify source table is accessible via linked dataset
        $linkedTable = $targetDataset->table(self::TESTTABLE_BEFORE_NAME);
        $this->assertTrue($linkedTable->exists());
        $linkedTableRows = iterator_to_array($linkedTable->rows());
        $this->assertCount(3, $linkedTableRows);

        // 6. KEY TEST: Verify VIEW is accessible via linked dataset
        $linkedView = $targetDataset->table($viewName);
        $this->assertTrue($linkedView->exists(), 'VIEW should be visible in linked dataset');
        $linkedViewRows = $this->queryView($targetBqClient, $linkedBucketSchemaName, $viewName);
        $this->assertCount(3, $linkedViewRows, 'VIEW via linked dataset should return all 3 rows');

        // 7. Verify filtered VIEW is accessible via linked dataset
        $linkedFilteredView = $targetDataset->table($filteredViewName);
        $this->assertTrue($linkedFilteredView->exists(), 'Filtered VIEW should be visible in linked dataset');
        $linkedFilteredRows = $this->queryView($targetBqClient, $linkedBucketSchemaName, $filteredViewName);
        $this->assertCount(2, $linkedFilteredRows, 'Filtered VIEW via linked dataset should return 2 rows');

        // 8. Create workspace in target project and load VIEW via LoadTableToWorkspaceHandler
        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $workspaceDataset = $workspaceResponse->getWorkspaceObjectName();

        // 9. Load VIEW from linked dataset into workspace using FULL import
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

        // 10. Load filtered VIEW from linked dataset into workspace
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

        // 11. Verify workspace user can see loaded tables and read from them
        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $workspaceCredentials);

        // WS user can see and read the table loaded from VIEW
        $wsUserTable = $wsBqClient->dataset($workspaceDataset)->table($wsLoadDestTable);
        $this->assertTrue($wsUserTable->exists(), 'WS user should see table loaded from VIEW');
        $wsUserRows = $this->queryView($wsBqClient, $workspaceDataset, $wsLoadDestTable);
        $this->assertCount(3, $wsUserRows, 'WS user should read 3 rows from table loaded from VIEW');

        // WS user can see and read the table loaded from filtered VIEW
        $wsUserFilteredTable = $wsBqClient->dataset($workspaceDataset)->table($wsLoadFilteredTable);
        $this->assertTrue($wsUserFilteredTable->exists(), 'WS user should see table loaded from filtered VIEW');
        $wsUserFilteredRows = $this->queryView($wsBqClient, $workspaceDataset, $wsLoadFilteredTable);
        $this->assertCount(2, $wsUserFilteredRows, 'WS user should read 2 rows from table loaded from filtered VIEW');

        // 12. Verify workspace user can read VIEWs in linked dataset directly via RO storage access
        $wsLinkedViewRows = $this->queryView($wsBqClient, $linkedBucketSchemaName, $viewName);
        $this->assertCount(3, $wsLinkedViewRows, 'WS user should read VIEW in linked dataset directly');

        $wsLinkedFilteredRows = $this->queryView($wsBqClient, $linkedBucketSchemaName, $filteredViewName);
        $this->assertCount(2, $wsLinkedFilteredRows, 'WS user should read filtered VIEW in linked dataset directly');

        // WS user can also read the source table in linked dataset directly
        $wsLinkedTableRows = $this->queryView($wsBqClient, $linkedBucketSchemaName, self::TESTTABLE_BEFORE_NAME);
        $this->assertCount(3, $wsLinkedTableRows, 'WS user should read source table in linked dataset directly');

        // 13. Cleanup: unlink -> unshare
        $this->cleanupLinkedDataset(
            $linkedBucketSchemaName,
            $listing,
        );
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
