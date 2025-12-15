<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace\Load;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnLink\UnLinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\LoadTableToWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\ObjectAlreadyExistsException;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketResponse;
use Keboola\StorageDriver\Command\Bucket\UnlinkBucketCommand;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use const JSON_THROW_ON_ERROR;

class LoadViewCloneTest extends BaseCase
{
    /**
     * @return Generator<string,array{ImportOptions\ImportType::*}>
     */
    public function importTypeProvider(): Generator
    {
        yield 'CLONE' => [
            ImportOptions\ImportType::PBCLONE,
        ];
        yield 'VIEW' => [
            ImportOptions\ImportType::VIEW,
        ];
    }

    /**
     * @dataProvider importTypeProvider
     * @param ImportOptions\ImportType::* $importType
     */
    public function testConflictImport(int $importType): void
    {
        // create resources
        $bucketResponse = $this->createTestBucket($this->projects[0][0]);
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projects[0][0]);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_Test_table';
        $this->createSourceTable(
            $bucketDatabaseName,
            $sourceTableName,
            $bqClient,
        );
        // create also destination so table exists
        $this->createSourceTable(
            $bucketDatabaseName,
            $sourceTableName . '_dest',
            $bqClient,
        );

        // import
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($sourceTableName . '_dest'),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType($importType),
        );
        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        try {
            $handler(
                $this->projects[0][0],
                $cmd,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('Should throw exception');
        } catch (ObjectAlreadyExistsException $e) {
            $this->assertSame(2006, $e->getCode());
        }

        // try again with replace mode
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType($importType)
                ->setCreateMode(ImportOptions\CreateMode::REPLACE),
        );
        $response = $handler(
            $this->projects[0][0],
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        //check response
        $this->assertInstanceOf(TableImportResponse::class, $response);
        $this->assertSame(0, $response->getImportedRowsCount());
        $this->assertSame(
            [],
            iterator_to_array($response->getImportedColumns()),
        );

        // check table read
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $sourceTableName . '_dest');
        if ($importType === ImportOptions\ImportType::VIEW) {
            // rest api is not returning rows count for views
            $this->assertSame(0, $ref->getRowsCount());
        } else {
            $this->assertSame(3, $ref->getRowsCount());
        }
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        $this->assertViewOrTableRowsCount(
            $bqClient,
            $bucketDatabaseName,
            $sourceTableName . '_dest',
            3,
        );
    }

    /**
     * @dataProvider importTypeProvider
     * @param ImportOptions\ImportType::* $importType
     */
    public function testImportAsView(int $importType): void
    {
        // create resources
        $bucketResponse = $this->createTestBucket($this->projects[0][0]);
        $destinationTableName = $this->getTestHash() . '_Test_table_final';
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projects[0][0]);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_Test_table';
        $qb = new BigqueryTableQueryBuilder();
        $this->createSourceTable(
            $bucketDatabaseName,
            $sourceTableName,
            $bqClient,
        );

        // import
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType($importType),
        );
        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projects[0][0],
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        //check response
        $this->assertSame(0, $response->getImportedRowsCount());
        $this->assertSame(
            [],
            iterator_to_array($response->getImportedColumns()),
        );

        // check table read
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        if ($importType === ImportOptions\ImportType::VIEW) {
            // rest api is not returning rows count for views
            $this->assertSame(0, $ref->getRowsCount());
        } else {
            $this->assertSame(3, $ref->getRowsCount());
        }
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        $this->assertViewOrTableRowsCount(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            3,
        );

        // cleanup
        if ($importType === ImportOptions\ImportType::VIEW) {
            $bqClient->runQuery($bqClient->query(
                sprintf(
                    'DROP VIEW %s.%s',
                    BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                    BigqueryQuote::quoteSingleIdentifier($destinationTableName),
                ),
            ));
        } else {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand(
                    $bucketDatabaseName,
                    $destinationTableName,
                ),
            ));
        }

        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand(
                $bucketDatabaseName,
                $sourceTableName,
            ),
        ));
    }

    /**
     * @dataProvider importTypeProvider
     * @param ImportOptions\ImportType::* $importType
     */
    public function testImportAsViewSharedBucket(int $importType): void
    {
        $destinationTableName = $this->getTestHash() . '_Test_table_final';
        //create linked bucket with table
        [
            $targetProjectCredentials,
            $targetProjectResponse,
            $linkedBucketDataset,
            $linkedBucketTableName,
            $cleanUp,
        ] = $this->createLinkedBucketWithTable();
        // create workspace to import into
        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $targetProjectCredentials,
            $targetProjectResponse,
        );
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $targetProjectCredentials);

        // import
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $linkedBucketDataset;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($linkedBucketTableName),
        );
        $destPath = new RepeatedField(GPBType::STRING);
        $destPath[] = $workspaceResponse->getWorkspaceObjectName();
        $cmd->setDestination(
            (new Table())
                ->setPath($destPath)
                ->setTableName($destinationTableName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType($importType),
        );
        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        /** @var TableImportResponse $response */
        $response = $handler(
            $targetProjectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        // check response
        if ($importType === ImportOptions\ImportType::VIEW) {
            $this->assertSame(0, $response->getImportedRowsCount());
        } else {
            // clone will fallback to CTAS and number of rows will be shown
            $this->assertSame(3, $response->getImportedRowsCount());
        }
        $this->assertSame(
            [],
            iterator_to_array($response->getImportedColumns()),
        );
        // check table read
        if ($importType === ImportOptions\ImportType::VIEW) {
            // rest api is not returning rows count for views
            $this->assertSame(0, $response->getImportedRowsCount());
        } else {
            $this->assertSame(3, $response->getImportedRowsCount());
        }
        $ref = new BigqueryTableReflection(
            $bqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            $destinationTableName,
        );
        // this will be also 0 for view but will match result from reflection
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        $this->assertViewOrTableRowsCount(
            $bqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            $destinationTableName,
            3,
        );

        // check table read as WS user
        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $workspaceCredentials);
        $ref = new BigqueryTableReflection(
            $wsBqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            $destinationTableName,
        );
        if ($importType === ImportOptions\ImportType::VIEW) {
            // rest api is not returning rows count for views
            $this->assertSame(0, $ref->getRowsCount());
        } else {
            $this->assertSame(3, $ref->getRowsCount());
        }
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        $this->assertViewOrTableRowsCount(
            $wsBqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            $destinationTableName,
            3,
        );

        // cleanup
        if ($importType === ImportOptions\ImportType::VIEW) {
            $bqClient->runQuery($bqClient->query(
                sprintf(
                    'DROP VIEW %s.%s',
                    BigqueryQuote::quoteSingleIdentifier($workspaceResponse->getWorkspaceObjectName()),
                    BigqueryQuote::quoteSingleIdentifier($destinationTableName),
                ),
            ));
        } else {
            $bqClient->runQuery($bqClient->query(
                (new BigqueryTableQueryBuilder())->getDropTableCommand(
                    $workspaceResponse->getWorkspaceObjectName(),
                    $destinationTableName,
                ),
            ));
        }
        $cleanUp();
    }

    private function createSourceTable(
        string $bucketDatabaseName,
        string $sourceTableName,
        BigQueryClient $bqClient,
    ): void {
        // create tables
        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $sourceTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createGenericColumn('col3'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));
        $insert = [];
        foreach ([['1', '1', '1'], ['2', '2', '2'], ['3', '3', '3']] as $i) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = BigqueryQuote::quote($item);
            }
            $insert[] = sprintf('(%s)', implode(',', $quotedValues));
        }

        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            implode(',', $insert),
        )));
    }

    /**
     * @return array{GenericBackendCredentials, CreateProjectResponse,string,string,callable}
     */
    private function createLinkedBucketWithTable(): array
    {
        $bucketResponse = $this->createTestBucket($this->projects[0][0]);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projects[0][0]);
        $linkedBucketSchemaName = $bucketDatabaseName . '_LINKED';

        // create source table to be shared
        $this->createSourceTable(
            $bucketDatabaseName,
            'sharedTable',
            $sourceBqClient,
        );

        // share the bucket
        $publicPart = (array) json_decode(
            $this->projects[0][1]->getProjectUserName(),
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
            ->setSourceBucketId('1234567')
            ->setSourceProjectReadOnlyRoleName($this->projects[0][1]->getProjectReadOnlyRoleName());

        $meta = new Any();
        $meta->pack(new ShareBucketCommand\ShareBucketBigqueryCommandMeta());
        $command->setMeta($meta);
        /** @var ShareBucketResponse $result */
        $result = $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        // link the bucket
        $listing = $result->getBucketShareRoleName();
        $publicPart = (array) json_decode(
            $this->projects[1][1]->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $targetProjectId */
        $targetProjectId = $publicPart['project_id'];
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
        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $credentials = $this->projects[1][0];
        return [
            $this->projects[1][0],
            $this->projects[1][1],
            $linkedBucketSchemaName,
            'sharedTable',
            function () use ($linkedBucketSchemaName, $credentials): void {
                $unlinkHandler = new UnLinkBucketHandler($this->clientManager);
                $unlinkHandler->setInternalLogger($this->log);
                $command = (new UnLinkBucketCommand())
                    ->setBucketObjectName($linkedBucketSchemaName);

                $unlinkHandler(
                    $credentials,
                    $command,
                    [],
                    new RuntimeOptions(['runId' => $this->testRunId]),
                );
            },
        ];
    }

    /**
     * count rows by selecting whole table
     */
    private function assertViewOrTableRowsCount(
        BigQueryClient $bqClient,
        string $datasetName,
        string $tableName,
        int $expectedRowsCount,
    ): void {
        $result = $bqClient->runQuery($bqClient->query(
            sprintf(
                'SELECT * FROM %s.%s',
                BigqueryQuote::quoteSingleIdentifier($datasetName),
                BigqueryQuote::quoteSingleIdentifier($tableName),
            ),
        ));
        $this->assertCount($expectedRowsCount, $result);
    }

    /**
     * Test that VIEW import creates actual BigQuery VIEW, not a table
     */
    public function testViewCreatesActualView(): void
    {
        // create resources
        $bucketResponse = $this->createTestBucket($this->projects[0][0]);
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projects[0][0]);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $viewName = $this->getTestHash() . '_view';

        $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // Create VIEW
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($viewName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::VIEW),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projects[0][0],
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify it's a VIEW, not a table
        $dataset = $bqClient->dataset($bucketDatabaseName);
        $table = $dataset->table($viewName);
        $info = $table->info();

        $this->assertSame('VIEW', $info['type'], 'Created object should be a VIEW, not a TABLE');
        $this->assertArrayHasKey('view', $info, 'VIEW info should contain view definition');

        // Verify importedRowsCount is 0 for views
        $this->assertSame(0, $response->getImportedRowsCount());

        // Verify the view is queryable and returns correct data
        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
        )));
        $this->assertCount(3, $result);
    }

    /**
     * Test that VIEW SQL structure references the source table correctly
     */
    public function testViewReferencesSourceTableCorrectly(): void
    {
        // create resources
        $bucketResponse = $this->createTestBucket($this->projects[0][0]);
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projects[0][0]);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $viewName = $this->getTestHash() . '_view';

        $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // Create VIEW
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($viewName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::VIEW),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $handler(
            $this->projects[0][0],
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Get view definition
        $dataset = $bqClient->dataset($bucketDatabaseName);
        $table = $dataset->table($viewName);
        $info = $table->info();

        $this->assertArrayHasKey('view', $info);
        $viewDefinition = $info['view']['query'];

        // Verify the view definition contains SELECT * FROM source table
        $this->assertStringContainsString('SELECT', $viewDefinition);
        $this->assertStringContainsString('FROM', $viewDefinition);
        $this->assertStringContainsString($sourceTableName, $viewDefinition);
        $this->assertStringContainsString($bucketDatabaseName, $viewDefinition);
    }

    /**
     * Test successful CLONE operation returns zero importedRowsCount
     */
    public function testSuccessfulCloneReturnsZeroImportedRows(): void
    {
        // create resources
        $bucketResponse = $this->createTestBucket($this->projects[0][0]);
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projects[0][0]);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $destinationTableName = $this->getTestHash() . '_dest';

        $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // CLONE within same dataset should succeed
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::PBCLONE),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projects[0][0],
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Successful CLONE returns 0 importedRowsCount (lines 398-400 in handler)
        $this->assertSame(0, $response->getImportedRowsCount());

        // But table should exist and have data
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $ref->getRowsCount());
    }

    /**
     * Test CLONE fallback to CREATE TABLE AS SELECT when clone fails
     * Fallback happens with cross-dataset clones (e.g., linked buckets)
     */
    public function testCloneFallbackPopulatesImportedRowsCount(): void
    {
        //create linked bucket with table
        [
            $targetProjectCredentials,
            $targetProjectResponse,
            $linkedBucketDataset,
            $linkedBucketTableName,
            $cleanUp,
        ] = $this->createLinkedBucketWithTable();

        // create workspace to import into
        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $targetProjectCredentials,
            $targetProjectResponse,
        );
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $targetProjectCredentials);
        $destinationTableName = $this->getTestHash() . '_cloned';

        // Try to CLONE from linked bucket - this will trigger fallback
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $linkedBucketDataset;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($linkedBucketTableName),
        );
        $destPath = new RepeatedField(GPBType::STRING);
        $destPath[] = $workspaceResponse->getWorkspaceObjectName();
        $cmd->setDestination(
            (new Table())
                ->setPath($destPath)
                ->setTableName($destinationTableName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::PBCLONE),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $targetProjectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Fallback uses CREATE TABLE AS SELECT, so importedRowsCount should be populated
        // (not 0 like successful CLONE)
        $this->assertSame(3, $response->getImportedRowsCount(), 'Fallback should populate importedRowsCount');

        // Verify table was created and has correct data
        $ref = new BigqueryTableReflection(
            $bqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            $destinationTableName,
        );
        $this->assertSame(3, $ref->getRowsCount());

        $cleanUp();
    }

    /**
     * Test CLONE fallback creates proper table structure with all data
     */
    public function testCloneFallbackCreatesProperTable(): void
    {
        //create linked bucket with table
        [
            $targetProjectCredentials,
            $targetProjectResponse,
            $linkedBucketDataset,
            $linkedBucketTableName,
            $cleanUp,
        ] = $this->createLinkedBucketWithTable();

        // create workspace to import into
        [$workspaceCredentials, $workspaceResponse] = $this->createTestWorkspace(
            $targetProjectCredentials,
            $targetProjectResponse,
        );
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $targetProjectCredentials);
        $destinationTableName = $this->getTestHash() . '_cloned';

        // CLONE from linked bucket triggers fallback
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $linkedBucketDataset;
        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($linkedBucketTableName),
        );
        $destPath = new RepeatedField(GPBType::STRING);
        $destPath[] = $workspaceResponse->getWorkspaceObjectName();
        $cmd->setDestination(
            (new Table())
                ->setPath($destPath)
                ->setTableName($destinationTableName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::PBCLONE),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $handler(
            $targetProjectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify table structure and data
        $ref = new BigqueryTableReflection(
            $bqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            $destinationTableName,
        );
        $tableDef = $ref->getTableDefinition();

        // Should have 3 columns
        $this->assertCount(3, $tableDef->getColumnsDefinitions());

        // Verify data is queryable
        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s ORDER BY col1',
            BigqueryQuote::quoteSingleIdentifier($workspaceResponse->getWorkspaceObjectName()),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
        $rows = iterator_to_array($result);
        $this->assertCount(3, $rows);
        $this->assertSame('1', $rows[0]['col1']);
        $this->assertSame('2', $rows[1]['col1']);
        $this->assertSame('3', $rows[2]['col1']);

        $cleanUp();
    }

    /**
     * Test VIEW always includes all source columns (column mapping is ignored for views)
     * This is current implementation behavior - views use SELECT * regardless of column mappings
     */
    public function testViewIncludesAllSourceColumns(): void
    {
        // create resources
        $bucketResponse = $this->createTestBucket($this->projects[0][0]);
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projects[0][0]);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_source';
        $viewName = $this->getTestHash() . '_view';

        $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // Create VIEW with column mapping specified (only col1 and col3)
        // But VIEW implementation uses SELECT *, so all columns will be included
        $cmd = new LoadTableToWorkspaceCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col1')
            ->setDestinationColumnName('col1');
        $columnMappings[] = (new LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('col3')
            ->setDestinationColumnName('col3');

        $cmd->setSource(
            (new LoadTableToWorkspaceCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
                ->setColumnMappings($columnMappings),
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($viewName),
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::VIEW),
        );

        $handler = new LoadTableToWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $handler(
            $this->projects[0][0],
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify it's a VIEW
        $dataset = $bqClient->dataset($bucketDatabaseName);
        $table = $dataset->table($viewName);
        $info = $table->info();
        $this->assertSame('VIEW', $info['type']);

        // Query the view and verify ALL columns are present (views use SELECT *)
        $result = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
        )));

        $rows = iterator_to_array($result);
        $this->assertCount(3, $rows);

        // Verify ALL source columns are present (col1, col2, col3)
        // This is current implementation - column mappings are ignored for VIEWs
        $firstRow = $rows[0];
        $this->assertArrayHasKey('col1', $firstRow);
        $this->assertArrayHasKey('col2', $firstRow, 'col2 is included because VIEWs use SELECT *');
        $this->assertArrayHasKey('col3', $firstRow);
    }
}

