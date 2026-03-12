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
use Keboola\Datatype\Definition\Common;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnLink\UnLinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnShare\UnShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\AddColumnHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\AlterColumnHandler;
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
use Keboola\StorageDriver\Command\Table\AlterColumnCommand;
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
     * VIEW + filtered VIEW visible and queryable in linked dataset.
     */
    public function testViewAccessibleViaLinkedDataset(): void
    {
        $ctx = $this->createViewBucketInfrastructure('Test2', 'Bb');

        $targetDataset = $ctx->targetBqClient->dataset($ctx->linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());

        // VIEW is accessible via linked dataset
        $linkedView = $targetDataset->table($ctx->viewName);
        $this->assertTrue($linkedView->exists(), 'VIEW should be visible in linked dataset');
        $linkedViewRows = $this->queryView($ctx->targetBqClient, $ctx->linkedBucketSchemaName, $ctx->viewName);
        $this->assertCount(3, $linkedViewRows, 'VIEW via linked dataset should return all 3 rows');

        // Filtered VIEW is accessible via linked dataset
        $this->assertNotNull($ctx->filteredViewName);
        $linkedFilteredView = $targetDataset->table($ctx->filteredViewName);
        $this->assertTrue($linkedFilteredView->exists(), 'Filtered VIEW should be visible in linked dataset');
        $linkedFilteredRows = $this->queryView(
            $ctx->targetBqClient,
            $ctx->linkedBucketSchemaName,
            $ctx->filteredViewName,
        );
        $this->assertCount(2, $linkedFilteredRows, 'Filtered VIEW via linked dataset should return 2 rows');

        $this->cleanupViewBucketInfrastructure($ctx);
    }

    /**
     * Workspace load from VIEW + filtered VIEW, workspace user RO direct access.
     */
    public function testWorkspaceLoadAndDirectAccessToLinkedView(): void
    {
        $ctx = $this->createViewBucketInfrastructure('WsTest', 'BbWs');

        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $workspaceDataset = $workspaceResponse->getWorkspaceObjectName();

        // Load VIEW from linked dataset into workspace
        $loadResponse = $this->loadViewToWorkspace(
            $ctx->linkedBucketSchemaName,
            $ctx->viewName,
            $workspaceDataset,
            'WS_LOADED_FROM_VIEW',
        );
        $this->assertSame(3, $loadResponse->getImportedRowsCount());
        $wsLoadedRows = $this->queryView($ctx->targetBqClient, $workspaceDataset, 'WS_LOADED_FROM_VIEW');
        $this->assertCount(3, $wsLoadedRows, 'Workspace table loaded from VIEW should have 3 rows');

        // Load filtered VIEW from linked dataset into workspace
        $this->assertNotNull($ctx->filteredViewName);
        $loadFilteredResponse = $this->loadViewToWorkspace(
            $ctx->linkedBucketSchemaName,
            $ctx->filteredViewName,
            $workspaceDataset,
            'WS_LOADED_FROM_FILTERED_VIEW',
        );
        $this->assertSame(2, $loadFilteredResponse->getImportedRowsCount());
        $wsFilteredRows = $this->queryView($ctx->targetBqClient, $workspaceDataset, 'WS_LOADED_FROM_FILTERED_VIEW');
        $this->assertCount(2, $wsFilteredRows, 'Workspace table loaded from filtered VIEW should have 2 rows');

        // Verify workspace user can see loaded tables and read from them
        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $workspaceCredentials);

        $wsUserTable = $wsBqClient->dataset($workspaceDataset)->table('WS_LOADED_FROM_VIEW');
        $this->assertTrue($wsUserTable->exists(), 'WS user should see table loaded from VIEW');
        $wsUserRows = $this->queryView($wsBqClient, $workspaceDataset, 'WS_LOADED_FROM_VIEW');
        $this->assertCount(3, $wsUserRows, 'WS user should read 3 rows from table loaded from VIEW');

        $wsUserFilteredTable = $wsBqClient->dataset($workspaceDataset)->table('WS_LOADED_FROM_FILTERED_VIEW');
        $this->assertTrue($wsUserFilteredTable->exists(), 'WS user should see table loaded from filtered VIEW');
        $wsUserFilteredRows = $this->queryView($wsBqClient, $workspaceDataset, 'WS_LOADED_FROM_FILTERED_VIEW');
        $this->assertCount(2, $wsUserFilteredRows, 'WS user should read 2 rows from filtered VIEW');

        // Verify workspace user can read VIEWs in linked dataset directly
        $wsLinkedViewRows = $this->queryView($wsBqClient, $ctx->linkedBucketSchemaName, $ctx->viewName);
        $this->assertCount(3, $wsLinkedViewRows, 'WS user should read VIEW in linked dataset directly');

        $wsLinkedFilteredRows = $this->queryView(
            $wsBqClient,
            $ctx->linkedBucketSchemaName,
            $ctx->filteredViewName,
        );
        $this->assertCount(2, $wsLinkedFilteredRows, 'WS user should read filtered VIEW in linked dataset directly');

        $this->cleanupViewBucketInfrastructure($ctx);
    }

    /**
     * Add column AGE to source table, verify through linked VIEW,
     * CREATE OR REPLACE VIEW to refresh metadata, workspace load.
     */
    public function testAddColumnReflectedViaLinkedView(): void
    {
        $ctx = $this->createViewBucketInfrastructure('TestAddCol', 'BbAddCol', createFilteredView: false);

        // Add column AGE to source table
        $addColumnHandler = new AddColumnHandler($this->clientManager);
        $addColumnHandler->setInternalLogger($this->log);
        $addColumnCommand = (new AddColumnCommand())
            ->setPath($ctx->baPath)
            ->setTableName($ctx->tableName)
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
        $ctx->sourceBqClient->runQuery($ctx->sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`, `NAME`, `AGE`) VALUES (%s, %s, %s)',
            BigqueryQuote::quoteSingleIdentifier($ctx->bucketBaName),
            BigqueryQuote::quoteSingleIdentifier($ctx->tableName),
            BigqueryQuote::quote('4'),
            BigqueryQuote::quote('dave'),
            BigqueryQuote::quote('25'),
        )));

        // Verify VIEW via linked dataset: 4 rows, AGE column present
        $linkedViewRows = $this->queryView($ctx->targetBqClient, $ctx->linkedBucketSchemaName, $ctx->viewName);
        $this->assertCount(4, $linkedViewRows, 'VIEW should return 4 rows after adding column and row');
        $this->assertViewColumns($linkedViewRows[0], ['ID', 'NAME', 'AGE'], [], 'Linked VIEW after add column');

        // Recreate VIEW to refresh BigQuery metadata (frozen at creation time)
        ($ctx->createViewHandler)(
            $this->sourceProjectCredentials,
            $ctx->createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Load VIEW into workspace after add column
        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $loadResponse = $this->loadViewToWorkspace(
            $ctx->linkedBucketSchemaName,
            $ctx->viewName,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_ADD_COL',
        );
        $this->assertSame(4, $loadResponse->getImportedRowsCount());
        $wsLoadedRows = $this->queryView(
            $ctx->targetBqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_ADD_COL',
        );
        $this->assertCount(4, $wsLoadedRows, 'WS loaded table should have 4 rows after add column');
        $this->assertViewColumns($wsLoadedRows[0], ['ID', 'NAME', 'AGE'], [], 'WS after add column');

        $this->cleanupViewBucketInfrastructure($ctx);
    }

    /**
     * Drop column NAME from source table, verify through linked VIEW,
     * CREATE OR REPLACE VIEW to refresh metadata, workspace load.
     */
    public function testDropColumnReflectedViaLinkedView(): void
    {
        $ctx = $this->createViewBucketInfrastructure('TestDropCol', 'BbDropCol', createFilteredView: false);

        // Drop column NAME from source table
        $dropColumnHandler = new DropColumnHandler($this->clientManager);
        $dropColumnHandler->setInternalLogger($this->log);
        $dropColumnCommand = (new DropColumnCommand())
            ->setPath($ctx->baPath)
            ->setTableName($ctx->tableName)
            ->setColumnName('NAME');
        $dropColumnHandler(
            $this->sourceProjectCredentials,
            $dropColumnCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify VIEW via linked dataset: 3 rows, only ID column
        $linkedViewRows = $this->queryView($ctx->targetBqClient, $ctx->linkedBucketSchemaName, $ctx->viewName);
        $this->assertCount(3, $linkedViewRows, 'VIEW should still return 3 rows after dropping column');
        $this->assertViewColumns($linkedViewRows[0], ['ID'], ['NAME'], 'Linked VIEW after drop column');

        // Recreate VIEW to refresh BigQuery metadata
        ($ctx->createViewHandler)(
            $this->sourceProjectCredentials,
            $ctx->createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Load VIEW into workspace after drop column
        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $loadResponse = $this->loadViewToWorkspace(
            $ctx->linkedBucketSchemaName,
            $ctx->viewName,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_DROP_COL',
        );
        $this->assertSame(3, $loadResponse->getImportedRowsCount());
        $wsLoadedRows = $this->queryView(
            $ctx->targetBqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_DROP_COL',
        );
        $this->assertCount(3, $wsLoadedRows, 'WS loaded table should have 3 rows after drop column');
        $this->assertViewColumns($wsLoadedRows[0], ['ID'], ['NAME'], 'WS after drop column');

        $this->cleanupViewBucketInfrastructure($ctx);
    }

    /**
     * Alter NAME column from REQUIRED to NULLABLE, verify through linked VIEW,
     * CREATE OR REPLACE VIEW to refresh metadata, workspace load.
     */
    public function testAlterColumnNullableReflectedViaLinkedView(): void
    {
        $ctx = $this->createViewBucketInfrastructure(
            'TestAlterNull',
            'BbAlterNull',
            createFilteredView: false,
            nameNullable: false,
        );

        // Alter NAME column from REQUIRED to NULLABLE
        $alterHandler = new AlterColumnHandler($this->clientManager);
        $alterHandler->setInternalLogger($this->log);
        $fields = new RepeatedField(GPBType::STRING);
        $fields[] = Common::KBC_METADATA_KEY_NULLABLE;
        $alterCommand = (new AlterColumnCommand())
            ->setPath($ctx->baPath)
            ->setTableName($ctx->tableName)
            ->setAttributesToUpdate($fields)
            ->setDesiredDefiniton(
                (new TableColumnShared())
                    ->setType(Bigquery::TYPE_STRING)
                    ->setName('NAME')
                    ->setNullable(true),
            );
        $alterHandler(
            $this->sourceProjectCredentials,
            $alterCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify VIEW via linked dataset still returns correct data
        $linkedViewRows = $this->queryView($ctx->targetBqClient, $ctx->linkedBucketSchemaName, $ctx->viewName);
        $this->assertCount(3, $linkedViewRows, 'VIEW should return 3 rows after altering nullable');
        $this->assertViewColumns($linkedViewRows[0], ['ID', 'NAME'], [], 'Linked VIEW after alter nullable');

        // Recreate VIEW to refresh BigQuery metadata
        ($ctx->createViewHandler)(
            $this->sourceProjectCredentials,
            $ctx->createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Load VIEW into workspace after alter
        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $loadResponse = $this->loadViewToWorkspace(
            $ctx->linkedBucketSchemaName,
            $ctx->viewName,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_ALTER_NULL',
        );
        $this->assertSame(3, $loadResponse->getImportedRowsCount());

        $this->cleanupViewBucketInfrastructure($ctx);
    }

    /**
     * Alter NAME column from STRING(50) to STRING(200), verify through linked VIEW,
     * CREATE OR REPLACE VIEW to refresh metadata, workspace load.
     */
    public function testAlterColumnLengthReflectedViaLinkedView(): void
    {
        $ctx = $this->createViewBucketInfrastructure(
            'TestAlterLen',
            'BbAlterLen',
            createFilteredView: false,
            nameLength: '50',
        );

        // Alter NAME column length from 50 to 200
        $alterHandler = new AlterColumnHandler($this->clientManager);
        $alterHandler->setInternalLogger($this->log);
        $fields = new RepeatedField(GPBType::STRING);
        $fields[] = Common::KBC_METADATA_KEY_LENGTH;
        $alterCommand = (new AlterColumnCommand())
            ->setPath($ctx->baPath)
            ->setTableName($ctx->tableName)
            ->setAttributesToUpdate($fields)
            ->setDesiredDefiniton(
                (new TableColumnShared())
                    ->setType(Bigquery::TYPE_STRING)
                    ->setName('NAME')
                    ->setLength('200'),
            );
        $alterHandler(
            $this->sourceProjectCredentials,
            $alterCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify VIEW via linked dataset still returns correct data
        $linkedViewRows = $this->queryView($ctx->targetBqClient, $ctx->linkedBucketSchemaName, $ctx->viewName);
        $this->assertCount(3, $linkedViewRows, 'VIEW should return 3 rows after altering length');
        $this->assertViewColumns($linkedViewRows[0], ['ID', 'NAME'], [], 'Linked VIEW after alter length');

        // Recreate VIEW to refresh BigQuery metadata
        ($ctx->createViewHandler)(
            $this->sourceProjectCredentials,
            $ctx->createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Load VIEW into workspace after alter
        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $loadResponse = $this->loadViewToWorkspace(
            $ctx->linkedBucketSchemaName,
            $ctx->viewName,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_ALTER_LEN',
        );
        $this->assertSame(3, $loadResponse->getImportedRowsCount());

        $this->cleanupViewBucketInfrastructure($ctx);
    }

    /**
     * Column subset VIEW (SELECT ID, NAME) is stable when a new column
     * is added to the source table -- the VIEW ignores new columns.
     */
    public function testColumnSubsetViewWithAddColumn(): void
    {
        $ctx = $this->createViewBucketInfrastructure(
            'TestSubsetAdd',
            'BbSubsetAdd',
            createFilteredView: false,
            viewColumns: ['ID', 'NAME'],
        );

        // Add column AGE to source table
        $addColumnHandler = new AddColumnHandler($this->clientManager);
        $addColumnHandler->setInternalLogger($this->log);
        $addColumnCommand = (new AddColumnCommand())
            ->setPath($ctx->baPath)
            ->setTableName($ctx->tableName)
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
        $ctx->sourceBqClient->runQuery($ctx->sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`, `NAME`, `AGE`) VALUES (%s, %s, %s)',
            BigqueryQuote::quoteSingleIdentifier($ctx->bucketBaName),
            BigqueryQuote::quoteSingleIdentifier($ctx->tableName),
            BigqueryQuote::quote('4'),
            BigqueryQuote::quote('dave'),
            BigqueryQuote::quote('25'),
        )));

        // Verify column subset VIEW returns only ID, NAME -- AGE is NOT visible
        $linkedViewRows = $this->queryView($ctx->targetBqClient, $ctx->linkedBucketSchemaName, $ctx->viewName);
        $this->assertCount(4, $linkedViewRows, 'Column subset VIEW should return 4 rows after add column');
        $this->assertViewColumns(
            $linkedViewRows[0],
            ['ID', 'NAME'],
            ['AGE'],
            'Column subset VIEW should not expose new AGE column',
        );

        $this->cleanupViewBucketInfrastructure($ctx);
    }

    /**
     * Column subset VIEW (SELECT ID, NAME) breaks when a referenced column
     * is dropped. After recreating with updated column list, the VIEW works again.
     */
    public function testColumnSubsetViewWithDropColumn(): void
    {
        $ctx = $this->createViewBucketInfrastructure(
            'TestSubsetDrop',
            'BbSubsetDrop',
            createFilteredView: false,
            viewColumns: ['ID', 'NAME'],
        );

        // Drop column NAME from source table
        $dropColumnHandler = new DropColumnHandler($this->clientManager);
        $dropColumnHandler->setInternalLogger($this->log);
        $dropColumnCommand = (new DropColumnCommand())
            ->setPath($ctx->baPath)
            ->setTableName($ctx->tableName)
            ->setColumnName('NAME');
        $dropColumnHandler(
            $this->sourceProjectCredentials,
            $dropColumnCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify VIEW fails -- it references non-existent column NAME
        $queryFailed = false;
        try {
            $this->queryView($ctx->targetBqClient, $ctx->linkedBucketSchemaName, $ctx->viewName);
        } catch (Throwable) {
            $queryFailed = true;
        }
        $this->assertTrue($queryFailed, 'Column subset VIEW referencing dropped column NAME should fail');

        // Recreate VIEW with updated column list (only ID)
        $newViewCommand = (new CreateViewCommand())
            ->setPath([$ctx->bucketBbName])
            ->setSourcePath([$ctx->bucketBaName])
            ->setViewName($ctx->viewName)
            ->setSourceTableName($ctx->tableName)
            ->setColumns(['ID']);
        ($ctx->createViewHandler)(
            $this->sourceProjectCredentials,
            $newViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify VIEW works again with only ID column
        $linkedViewRows = $this->queryView($ctx->targetBqClient, $ctx->linkedBucketSchemaName, $ctx->viewName);
        $this->assertCount(3, $linkedViewRows, 'Recreated VIEW should return 3 rows');
        $this->assertViewColumns(
            $linkedViewRows[0],
            ['ID'],
            ['NAME'],
            'Recreated VIEW should only have ID column',
        );

        $this->cleanupViewBucketInfrastructure($ctx);
    }

    // ------------------------------------------------------------------
    // Helper methods
    // ------------------------------------------------------------------

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

    /**
     * Full infrastructure setup: creates Ba+Bb buckets, table with data,
     * VIEW(s), shares Bb via Analytics Hub, and links in target project.
     *
     * @param string[] $viewColumns Column subset for VIEW; empty = SELECT *
     */
    private function createViewBucketInfrastructure(
        string $bbSuffix,
        string $bbShareSuffix,
        bool $createFilteredView = true,
        bool $nameNullable = true,
        ?string $nameLength = null,
        array $viewColumns = [],
    ): ViewBucketTestContext {
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

        // Compute Bb names
        $bucketBbId = $this->getTestHash() . 'in.c-' . $bbSuffix;
        $bucketBbShareId = $this->getTestHash() . $bbShareSuffix;
        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $expectedBbDatasetName = $nameGenerator->createObjectNameForBucketInProject($bucketBbId, null);
        $linkedBucketSchemaName = $expectedBbDatasetName . '_LINKED';

        // --- Stale cleanup ---
        try {
            $staleLinkedDataset = $targetBqClient->dataset($linkedBucketSchemaName);
            if ($staleLinkedDataset->exists()) {
                $staleLinkedDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

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

        try {
            $staleBbDataset = $sourceBqClient->dataset($expectedBbDatasetName);
            if ($staleBbDataset->exists()) {
                $staleBbDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        // --- Create Ba and Bb buckets ---
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

        // --- Create table in Ba ---
        $tableName = self::TESTTABLE_BEFORE_NAME;
        $tableHandler = new CreateTableHandler($this->clientManager);
        $tableHandler->setInternalLogger($this->log);
        $baPath = new RepeatedField(GPBType::STRING);
        $baPath[] = $bucketBaName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared())
            ->setName('ID')
            ->setType(Bigquery::TYPE_STRING);
        $nameCol = (new TableColumnShared())
            ->setName('NAME')
            ->setType(Bigquery::TYPE_STRING)
            ->setNullable($nameNullable);
        if ($nameLength !== null) {
            $nameCol->setLength($nameLength);
        }
        $columns[] = $nameCol;
        $command = (new CreateTableCommand())
            ->setPath($baPath)
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

        // --- Create VIEW in Bb -> Ba ---
        $viewName = 'ALIAS_VIEW';
        $createViewHandler = new CreateViewHandler($this->clientManager);
        $createViewHandler->setInternalLogger($this->log);
        $createViewCommand = (new CreateViewCommand())
            ->setPath([$bucketBbName])
            ->setSourcePath([$bucketBaName])
            ->setViewName($viewName)
            ->setSourceTableName($tableName);
        if ($viewColumns !== []) {
            $createViewCommand->setColumns($viewColumns);
        }
        $createViewHandler(
            $this->sourceProjectCredentials,
            $createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // --- Create filtered VIEW (if requested) ---
        $filteredViewName = null;
        if ($createFilteredView) {
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
        }

        // --- Share Bb via Analytics Hub ---
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

        // --- Link Bb in target project ---
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

        // Re-fetch target BQ client after linking
        $targetBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );

        return new ViewBucketTestContext(
            sourceProjectId: $sourceProjectId,
            bucketBaName: $bucketBaName,
            bucketBbName: $bucketBbName,
            linkedBucketSchemaName: $linkedBucketSchemaName,
            listing: $listing,
            viewName: $viewName,
            filteredViewName: $filteredViewName,
            tableName: $tableName,
            sourceBqClient: $sourceBqClient,
            targetBqClient: $targetBqClient,
            baPath: $baPath,
            createViewCommand: $createViewCommand,
            createViewHandler: $createViewHandler,
        );
    }

    private function cleanupViewBucketInfrastructure(ViewBucketTestContext $ctx): void
    {
        $this->cleanupLinkedDataset(
            $ctx->linkedBucketSchemaName,
            $ctx->listing,
        );

        try {
            $bbDataset = $ctx->sourceBqClient->dataset($ctx->bucketBbName);
            if ($bbDataset->exists()) {
                $bbDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }
    }

    private function loadViewToWorkspace(
        string $sourceDataset,
        string $sourceViewName,
        string $workspaceDataset,
        string $destinationTableName,
    ): TableImportResponse {
        $loadCmd = new LoadTableToWorkspaceCommand();

        $sourcePath = new RepeatedField(GPBType::STRING);
        $sourcePath[] = $sourceDataset;
        $loadCmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($sourcePath)
                ->setTableName($sourceViewName),
        );

        $destPath = new RepeatedField(GPBType::STRING);
        $destPath[] = $workspaceDataset;
        $loadCmd->setDestination(
            (new Table())
                ->setPath($destPath)
                ->setTableName($destinationTableName),
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

        return $loadResponse;
    }

    /**
     * @param array<string, mixed> $row
     * @param string[] $expected
     * @param string[] $notExpected
     */
    private function assertViewColumns(array $row, array $expected, array $notExpected, string $msg): void
    {
        foreach ($expected as $key) {
            $this->assertArrayHasKey($key, $row, "$msg: expected column $key");
        }
        foreach ($notExpected as $key) {
            $this->assertArrayNotHasKey($key, $row, "$msg: unexpected column $key");
        }
    }
}
