<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Cloud\Core\Exception\ServiceException;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Common;
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

class ShareLinkBucketViewTest extends BaseCase
{
    private const TESTTABLE_BEFORE_NAME = 'TESTTABLE_BEFORE';

    protected GenericBackendCredentials $sourceProjectCredentials;

    protected CreateProjectResponse $sourceProjectResponse;

    protected GenericBackendCredentials $targetProjectCredentials;

    protected CreateProjectResponse $targetProjectResponse;

    private ?ViewBucketTestContext $ctx = null;

    protected function setUp(): void
    {
        parent::setUp();
        $this->sourceProjectCredentials = $this->projects[0][0];
        $this->sourceProjectResponse = $this->projects[0][1];
        $this->targetProjectCredentials = $this->projects[1][0];
        $this->targetProjectResponse = $this->projects[1][1];
    }

    protected function tearDown(): void
    {
        if ($this->ctx !== null) {
            $this->cleanupViewBucketInfrastructure($this->ctx);
            $this->ctx = null;
        }
        parent::tearDown();
    }

    private function ctx(): ViewBucketTestContext
    {
        $this->assertNotNull($this->ctx, 'ViewBucketTestContext not initialized');

        return $this->ctx;
    }

    /**
     * VIEW visible and queryable in linked dataset.
     */
    public function testViewAccessibleViaLinkedDataset(): void
    {
        $this->ctx = $this->createViewBucketInfrastructure('Test2', 'Bb');
        $targetDataset = $this->ctx()->targetBqClient->dataset($this->ctx()->linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());

        // VIEW is accessible via linked dataset
        $linkedView = $targetDataset->table($this->ctx()->viewName);
        $this->assertTrue($linkedView->exists(), 'VIEW should be visible in linked dataset');
        $linkedViewRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
        );
        $this->assertCount(3, $linkedViewRows, 'VIEW via linked dataset should return all 3 rows');

        // Verify actual data values
        $ids = array_column($linkedViewRows, 'ID');
        sort($ids);
        $this->assertSame(['1', '2', '3'], $ids, 'VIEW should contain IDs 1, 2, 3');
    }

    /**
     * Workspace load from VIEW, workspace user RO direct access.
     */
    public function testWorkspaceLoadAndDirectAccessToLinkedView(): void
    {
        $this->ctx = $this->createViewBucketInfrastructure('WsTest', 'BbWs');
        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $workspaceDataset = $workspaceResponse->getWorkspaceObjectName();

        // Load VIEW from linked dataset into workspace
        $loadResponse = $this->loadViewToWorkspace(
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
            $workspaceDataset,
            'WS_LOADED_FROM_VIEW',
        );
        $this->assertSame(3, $loadResponse->getImportedRowsCount());
        $wsLoadedRows = $this->queryView($this->ctx()->targetBqClient, $workspaceDataset, 'WS_LOADED_FROM_VIEW');
        $this->assertCount(3, $wsLoadedRows, 'Workspace table loaded from VIEW should have 3 rows');

        // Verify workspace user can see loaded tables and read from them
        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $workspaceCredentials);

        $wsUserTable = $wsBqClient->dataset($workspaceDataset)->table('WS_LOADED_FROM_VIEW');
        $this->assertTrue($wsUserTable->exists(), 'WS user should see table loaded from VIEW');
        $wsUserRows = $this->queryView($wsBqClient, $workspaceDataset, 'WS_LOADED_FROM_VIEW');
        $this->assertCount(3, $wsUserRows, 'WS user should read 3 rows from table loaded from VIEW');

        // Verify workspace user can read VIEW in linked dataset directly
        $wsLinkedViewRows = $this->queryView($wsBqClient, $this->ctx()->linkedBucketSchemaName, $this->ctx()->viewName);
        $this->assertCount(3, $wsLinkedViewRows, 'WS user should read VIEW in linked dataset directly');
    }

    /**
     * Add column AGE to source table, verify through linked VIEW,
     * CREATE OR REPLACE VIEW to refresh metadata, workspace load.
     */
    public function testAddColumnReflectedViaLinkedView(): void
    {
        $this->ctx = $this->createViewBucketInfrastructure('TestAddCol', 'BbAddCol');

        // Add column AGE to source table
        $addColumnHandler = new AddColumnHandler($this->clientManager);
        $addColumnHandler->setInternalLogger($this->log);
        $addColumnCommand = (new AddColumnCommand())
            ->setPath($this->ctx()->baPath)
            ->setTableName($this->ctx()->tableName)
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
        $this->ctx()->sourceBqClient->runQuery($this->ctx()->sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`, `NAME`, `AGE`) VALUES (%s, %s, %s)',
            BigqueryQuote::quoteSingleIdentifier($this->ctx()->bucketBaName),
            BigqueryQuote::quoteSingleIdentifier($this->ctx()->tableName),
            BigqueryQuote::quote('4'),
            BigqueryQuote::quote('dave'),
            BigqueryQuote::quote('25'),
        )));

        // Verify VIEW via linked dataset: 4 rows, AGE column present
        $linkedViewRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
        );
        $this->assertCount(4, $linkedViewRows, 'VIEW should return 4 rows after adding column and row');
        $this->assertViewColumns($linkedViewRows[0], ['ID', 'NAME', 'AGE'], [], 'Linked VIEW after add column');

        // Verify the new row has correct data values
        $daveRow = $this->findRowById($linkedViewRows, '4');
        $this->assertNotNull($daveRow, 'Row with ID=4 should exist');
        $this->assertSame('dave', $daveRow['NAME'], 'New row NAME should be dave');
        $this->assertSame('25', $daveRow['AGE'], 'New row AGE should be 25');

        // Recreate VIEW to refresh BigQuery metadata (frozen at creation time)
        ($this->ctx()->createViewHandler)(
            $this->sourceProjectCredentials,
            $this->ctx()->createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Load VIEW into workspace after add column
        [, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $loadResponse = $this->loadViewToWorkspace(
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_ADD_COL',
        );
        $this->assertSame(4, $loadResponse->getImportedRowsCount());
        $wsLoadedRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_ADD_COL',
        );
        $this->assertCount(4, $wsLoadedRows, 'WS loaded table should have 4 rows after add column');
        $this->assertViewColumns($wsLoadedRows[0], ['ID', 'NAME', 'AGE'], [], 'WS after add column');
    }

    /**
     * Drop column NAME from source table, verify through linked VIEW,
     * CREATE OR REPLACE VIEW to refresh metadata, workspace load.
     */
    public function testDropColumnReflectedViaLinkedView(): void
    {
        $this->ctx = $this->createViewBucketInfrastructure('TestDropCol', 'BbDropCol');

        // Drop column NAME from source table
        $dropColumnHandler = new DropColumnHandler($this->clientManager);
        $dropColumnHandler->setInternalLogger($this->log);
        $dropColumnCommand = (new DropColumnCommand())
            ->setPath($this->ctx()->baPath)
            ->setTableName($this->ctx()->tableName)
            ->setColumnName('NAME');
        $dropColumnHandler(
            $this->sourceProjectCredentials,
            $dropColumnCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify VIEW via linked dataset: 3 rows, only ID column
        $linkedViewRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
        );
        $this->assertCount(3, $linkedViewRows, 'VIEW should still return 3 rows after dropping column');
        $this->assertViewColumns($linkedViewRows[0], ['ID'], ['NAME'], 'Linked VIEW after drop column');

        // Verify remaining column values
        $ids = array_column($linkedViewRows, 'ID');
        sort($ids);
        $this->assertSame(['1', '2', '3'], $ids, 'VIEW should contain IDs 1, 2, 3 after drop column');

        // Recreate VIEW to refresh BigQuery metadata
        ($this->ctx()->createViewHandler)(
            $this->sourceProjectCredentials,
            $this->ctx()->createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Load VIEW into workspace after drop column
        [, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $loadResponse = $this->loadViewToWorkspace(
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_DROP_COL',
        );
        $this->assertSame(3, $loadResponse->getImportedRowsCount());
        $wsLoadedRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_DROP_COL',
        );
        $this->assertCount(3, $wsLoadedRows, 'WS loaded table should have 3 rows after drop column');
        $this->assertViewColumns($wsLoadedRows[0], ['ID'], ['NAME'], 'WS after drop column');
    }

    /**
     * Alter NAME column from REQUIRED to NULLABLE, verify through linked VIEW,
     * CREATE OR REPLACE VIEW to refresh metadata, workspace load.
     */
    public function testAlterColumnNullableReflectedViaLinkedView(): void
    {
        $this->ctx = $this->createViewBucketInfrastructure(
            'TestAlterNull',
            'BbAlterNull',
            nameNullable: false,
        );

        // Alter NAME column from REQUIRED to NULLABLE
        $alterHandler = new AlterColumnHandler($this->clientManager);
        $alterHandler->setInternalLogger($this->log);
        $fields = new RepeatedField(GPBType::STRING);
        $fields[] = Common::KBC_METADATA_KEY_NULLABLE;
        $alterCommand = (new AlterColumnCommand())
            ->setPath($this->ctx()->baPath)
            ->setTableName($this->ctx()->tableName)
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
        $linkedViewRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
        );
        $this->assertCount(3, $linkedViewRows, 'VIEW should return 3 rows after altering nullable');
        $this->assertViewColumns($linkedViewRows[0], ['ID', 'NAME'], [], 'Linked VIEW after alter nullable');

        // Insert a row with NULL NAME to prove nullable change propagates through VIEW
        $this->ctx()->sourceBqClient->runQuery($this->ctx()->sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`, `NAME`) VALUES (%s, NULL)',
            BigqueryQuote::quoteSingleIdentifier($this->ctx()->bucketBaName),
            BigqueryQuote::quoteSingleIdentifier($this->ctx()->tableName),
            BigqueryQuote::quote('4'),
        )));

        // Verify VIEW returns 4 rows and the NULL row is visible
        $linkedViewRowsAfterNull = $this->queryView(
            $this->ctx()->targetBqClient,
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
        );
        $this->assertCount(4, $linkedViewRowsAfterNull, 'VIEW should return 4 rows after inserting NULL row');
        $nullRow = $this->findRowById($linkedViewRowsAfterNull, '4');
        $this->assertNotNull($nullRow, 'Row with ID=4 should exist');
        $this->assertNull($nullRow['NAME'], 'Row with ID=4 should have NULL NAME');

        // Recreate VIEW to refresh BigQuery metadata
        ($this->ctx()->createViewHandler)(
            $this->sourceProjectCredentials,
            $this->ctx()->createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Load VIEW into workspace after alter
        [, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $loadResponse = $this->loadViewToWorkspace(
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_ALTER_NULL',
        );
        $this->assertSame(4, $loadResponse->getImportedRowsCount());
    }

    /**
     * Alter NAME column from STRING(50) to STRING(200), verify through linked VIEW,
     * CREATE OR REPLACE VIEW to refresh metadata, workspace load.
     */
    public function testAlterColumnLengthReflectedViaLinkedView(): void
    {
        $this->ctx = $this->createViewBucketInfrastructure(
            'TestAlterLen',
            'BbAlterLen',
            nameLength: '50',
        );

        // Alter NAME column length from 50 to 200
        $alterHandler = new AlterColumnHandler($this->clientManager);
        $alterHandler->setInternalLogger($this->log);
        $fields = new RepeatedField(GPBType::STRING);
        $fields[] = Common::KBC_METADATA_KEY_LENGTH;
        $alterCommand = (new AlterColumnCommand())
            ->setPath($this->ctx()->baPath)
            ->setTableName($this->ctx()->tableName)
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
        $linkedViewRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
        );
        $this->assertCount(3, $linkedViewRows, 'VIEW should return 3 rows after altering length');
        $this->assertViewColumns($linkedViewRows[0], ['ID', 'NAME'], [], 'Linked VIEW after alter length');

        // Recreate VIEW to refresh BigQuery metadata
        ($this->ctx()->createViewHandler)(
            $this->sourceProjectCredentials,
            $this->ctx()->createViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Load VIEW into workspace after alter
        [, $workspaceResponse] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );
        $loadResponse = $this->loadViewToWorkspace(
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
            $workspaceResponse->getWorkspaceObjectName(),
            'WS_VIEW_AFTER_ALTER_LEN',
        );
        $this->assertSame(3, $loadResponse->getImportedRowsCount());
    }

    /**
     * Column subset VIEW (SELECT ID, NAME) is stable when a new column
     * is added to the source table -- the VIEW ignores new columns.
     */
    public function testColumnSubsetViewWithAddColumn(): void
    {
        $this->ctx = $this->createViewBucketInfrastructure(
            'TestSubsetAdd',
            'BbSubsetAdd',
            viewColumns: ['ID', 'NAME'],
        );

        // Add column AGE to source table
        $addColumnHandler = new AddColumnHandler($this->clientManager);
        $addColumnHandler->setInternalLogger($this->log);
        $addColumnCommand = (new AddColumnCommand())
            ->setPath($this->ctx()->baPath)
            ->setTableName($this->ctx()->tableName)
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
        $this->ctx()->sourceBqClient->runQuery($this->ctx()->sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`, `NAME`, `AGE`) VALUES (%s, %s, %s)',
            BigqueryQuote::quoteSingleIdentifier($this->ctx()->bucketBaName),
            BigqueryQuote::quoteSingleIdentifier($this->ctx()->tableName),
            BigqueryQuote::quote('4'),
            BigqueryQuote::quote('dave'),
            BigqueryQuote::quote('25'),
        )));

        // Verify column subset VIEW returns only ID, NAME -- AGE is NOT visible
        $linkedViewRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
        );
        $this->assertCount(4, $linkedViewRows, 'Column subset VIEW should return 4 rows after add column');
        $this->assertViewColumns(
            $linkedViewRows[0],
            ['ID', 'NAME'],
            ['AGE'],
            'Column subset VIEW should not expose new AGE column',
        );
    }

    /**
     * Column subset VIEW (SELECT ID, NAME) breaks when a referenced column
     * is dropped. After recreating with updated column list, the VIEW works again.
     */
    public function testColumnSubsetViewWithDropColumn(): void
    {
        $this->ctx = $this->createViewBucketInfrastructure(
            'TestSubsetDrop',
            'BbSubsetDrop',
            viewColumns: ['ID', 'NAME'],
        );

        // Drop column NAME from source table
        $dropColumnHandler = new DropColumnHandler($this->clientManager);
        $dropColumnHandler->setInternalLogger($this->log);
        $dropColumnCommand = (new DropColumnCommand())
            ->setPath($this->ctx()->baPath)
            ->setTableName($this->ctx()->tableName)
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
            $this->queryView(
                $this->ctx()->targetBqClient,
                $this->ctx()->linkedBucketSchemaName,
                $this->ctx()->viewName,
            );
        } catch (BadRequestException) {
            $queryFailed = true;
        }
        $this->assertTrue($queryFailed, 'Column subset VIEW referencing dropped column NAME should fail');

        // Recreate VIEW with updated column list (only ID)
        $newViewCommand = (new CreateViewCommand())
            ->setPath([$this->ctx()->bucketBbName])
            ->setSourcePath([$this->ctx()->bucketBaName])
            ->setViewName($this->ctx()->viewName)
            ->setSourceTableName($this->ctx()->tableName)
            ->setColumns(['ID']);
        ($this->ctx()->createViewHandler)(
            $this->sourceProjectCredentials,
            $newViewCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify VIEW works again with only ID column
        $linkedViewRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
        );
        $this->assertCount(3, $linkedViewRows, 'Recreated VIEW should return 3 rows');
        $this->assertViewColumns(
            $linkedViewRows[0],
            ['ID'],
            ['NAME'],
            'Recreated VIEW should only have ID column',
        );
    }

    /**
     * UPDATE and DELETE on source table propagate through linked VIEW.
     */
    public function testDataChangesReflectedViaLinkedView(): void
    {
        $this->ctx = $this->createViewBucketInfrastructure('TestDataChg', 'BbDataChg');

        // Verify initial state: 3 rows
        $linkedViewRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
        );
        $this->assertCount(3, $linkedViewRows, 'VIEW should return 3 rows initially');

        // UPDATE: change alice -> alice_updated
        $this->ctx()->sourceBqClient->runQuery($this->ctx()->sourceBqClient->query(sprintf(
            'UPDATE %s.%s SET `NAME` = %s WHERE `ID` = %s',
            BigqueryQuote::quoteSingleIdentifier($this->ctx()->bucketBaName),
            BigqueryQuote::quoteSingleIdentifier($this->ctx()->tableName),
            BigqueryQuote::quote('alice_updated'),
            BigqueryQuote::quote('1'),
        )));

        // Verify UPDATE via linked VIEW
        $linkedViewRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
        );
        $this->assertCount(3, $linkedViewRows, 'VIEW should still return 3 rows after UPDATE');
        $updatedRow = $this->findRowById($linkedViewRows, '1');
        $this->assertNotNull($updatedRow, 'Row with ID=1 should exist after UPDATE');
        $this->assertSame('alice_updated', $updatedRow['NAME'], 'NAME should be updated to alice_updated');

        // DELETE: remove row ID='1'
        $this->ctx()->sourceBqClient->runQuery($this->ctx()->sourceBqClient->query(sprintf(
            'DELETE FROM %s.%s WHERE `ID` = %s',
            BigqueryQuote::quoteSingleIdentifier($this->ctx()->bucketBaName),
            BigqueryQuote::quoteSingleIdentifier($this->ctx()->tableName),
            BigqueryQuote::quote('1'),
        )));

        // Verify DELETE via linked VIEW
        $linkedViewRows = $this->queryView(
            $this->ctx()->targetBqClient,
            $this->ctx()->linkedBucketSchemaName,
            $this->ctx()->viewName,
        );
        $this->assertCount(2, $linkedViewRows, 'VIEW should return 2 rows after DELETE');
        $ids = array_column($linkedViewRows, 'ID');
        sort($ids);
        $this->assertSame(['2', '3'], $ids, 'VIEW should contain IDs 2, 3 after DELETE');
    }

    /**
     * Workspace user should NOT be able to query source dataset Ba directly.
     */
    public function testWorkspaceUserCannotAccessSourceDatasetDirectly(): void
    {
        $this->ctx = $this->createViewBucketInfrastructure('TestWsSec', 'BbWsSec');
        [$workspaceCredentials] = $this->createTestWorkspace(
            $this->targetProjectCredentials,
            $this->targetProjectResponse,
        );

        // Workspace user can access linked dataset
        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $workspaceCredentials);
        $wsLinkedViewRows = $this->queryView($wsBqClient, $this->ctx()->linkedBucketSchemaName, $this->ctx()->viewName);
        $this->assertCount(3, $wsLinkedViewRows, 'WS user should read VIEW in linked dataset');

        // Workspace user should NOT be able to query source dataset Ba directly
        $this->expectException(ServiceException::class);
        $this->queryView($wsBqClient, $this->ctx()->bucketBaName, $this->ctx()->tableName);
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

    /**
     * @param list<array<string, mixed>> $rows
     * @return array<string, mixed>|null
     */
    private function findRowById(array $rows, string $id): ?array
    {
        foreach ($rows as $row) {
            if (($row['ID'] ?? null) === $id) {
                return $row;
            }
        }

        return null;
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
        $unshareCommand = (new UnshareBucketCommand())
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
