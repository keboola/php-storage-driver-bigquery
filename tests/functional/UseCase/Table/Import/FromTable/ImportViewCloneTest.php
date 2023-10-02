<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\FromTable;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnLink\UnLinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\ObjectAlreadyExistsException;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketResponse;
use Keboola\StorageDriver\Command\Bucket\UnlinkBucketCommand;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

class ImportViewCloneTest extends BaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

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
        [$projectCredentials,] = $this->createTestProject();
        $bucketResponse = $this->createTestBucket($projectCredentials);
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $projectCredentials);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceTableName = md5($this->getName()) . '_Test_table';
        $this->createSourceTable(
            $bucketDatabaseName,
            $sourceTableName,
            $bqClient
        );
        // create also destination so table exists
        $qb = new BigqueryTableQueryBuilder();
        $this->createSourceTable(
            $bucketDatabaseName,
            $sourceTableName . '_dest',
            $bqClient
        );

        // import
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($sourceTableName . '_dest')
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType($importType)
        );
        $handler = new ImportTableFromTableHandler($this->clientManager);
        try {
            $handler(
                $projectCredentials,
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
                ->setCreateMode(ImportOptions\CreateMode::REPLACE)
        );
        $response = $handler(
            $projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        //check response
        $this->assertInstanceOf(TableImportResponse::class, $response);
        $this->assertSame(0, $response->getImportedRowsCount());
        $this->assertSame(
            [],
            iterator_to_array($response->getImportedColumns())
        );

        // check table read
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $sourceTableName . '_dest');
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());
    }

    /**
     * @dataProvider importTypeProvider
     * @param ImportOptions\ImportType::* $importType
     */
    public function testImportAsView(int $importType): void
    {
        // create resources
        [$projectCredentials,] = $this->createTestProject();
        $bucketResponse = $this->createTestBucket($projectCredentials);
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $projectCredentials);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceTableName = md5($this->getName()) . '_Test_table';
        $qb = new BigqueryTableQueryBuilder();
        $this->createSourceTable(
            $bucketDatabaseName,
            $sourceTableName,
            $bqClient
        );

        // import
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType($importType)
        );
        $handler = new ImportTableFromTableHandler($this->clientManager);
        /** @var TableImportResponse $response */
        $response = $handler(
            $projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        //check response
        $this->assertSame(0, $response->getImportedRowsCount());
        $this->assertSame(
            [],
            iterator_to_array($response->getImportedColumns())
        );

        // check table read
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // cleanup
        if ($importType === ImportOptions\ImportType::VIEW) {
            $bqClient->runQuery($bqClient->query(
                sprintf(
                    'DROP VIEW %s.%s',
                    BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                    BigqueryQuote::quoteSingleIdentifier($destinationTableName)
                )
            ));
        } else {
            $bqClient->runQuery($bqClient->query(
                $qb->getDropTableCommand(
                    $bucketDatabaseName,
                    $destinationTableName
                )
            ));
        }

        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand(
                $bucketDatabaseName,
                $sourceTableName
            )
        ));
    }

    /**
     * @dataProvider importTypeProvider
     * @param ImportOptions\ImportType::* $importType
     */
    public function testImportAsViewSharedBucket(int $importType): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
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
            $targetProjectResponse
        );
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $targetProjectCredentials);

        // import
        $cmd = new TableImportFromTableCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $linkedBucketDataset;
        $cmd->setSource(
            (new TableImportFromTableCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($linkedBucketTableName)
        );
        $destPath = new RepeatedField(GPBType::STRING);
        $destPath[] = $workspaceResponse->getWorkspaceObjectName();
        $cmd->setDestination(
            (new Table())
                ->setPath($destPath)
                ->setTableName($destinationTableName)
        );
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType($importType)
        );
        $handler = new ImportTableFromTableHandler($this->clientManager);
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
            iterator_to_array($response->getImportedColumns())
        );
        // check table read
        $ref = new BigqueryTableReflection(
            $bqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            $destinationTableName
        );
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // check table read as WS user
        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $workspaceCredentials);
        $ref = new BigqueryTableReflection(
            $wsBqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            $destinationTableName
        );
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // cleanup
        if ($importType === ImportOptions\ImportType::VIEW) {
            $bqClient->runQuery($bqClient->query(
                sprintf(
                    'DROP VIEW %s.%s',
                    BigqueryQuote::quoteSingleIdentifier($workspaceResponse->getWorkspaceObjectName()),
                    BigqueryQuote::quoteSingleIdentifier($destinationTableName)
                )
            ));
        } else {
            $bqClient->runQuery($bqClient->query(
                (new BigqueryTableQueryBuilder())->getDropTableCommand(
                    $workspaceResponse->getWorkspaceObjectName(),
                    $destinationTableName
                )
            ));
        }
        $cleanUp();
    }

    private function createSourceTable(
        string $bucketDatabaseName,
        string $sourceTableName,
        BigQueryClient $bqClient
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
            []
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            $tableSourceDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));
        foreach ([['1', '1', '1'], ['2', '2', '2'], ['3', '3', '3']] as $i) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = BigqueryQuote::quote($item);
            }
            $bqClient->runQuery($bqClient->query(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($sourceTableName),
                implode(',', $quotedValues)
            )));
        }
    }

    /**
     * @return array{GenericBackendCredentials, CreateProjectResponse,string,string,callable}
     */
    private function createLinkedBucketWithTable(): array
    {
        parent::setUp();
        $this->cleanTestProject();

        [$sourceProjectCredentials, $sourceProjectResponse] = $this->createTestProject();
        $this->projectSuffix = '-s';
        [$targetProjectCredentials, $targetProjectResponse] = $this->createTestProject();

        $bucketResponse = $this->createTestBucket($sourceProjectCredentials);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $sourceProjectCredentials);
        $linkedBucketSchemaName = $bucketDatabaseName . '_LINKED';

        // create source table to be shared
        $this->createSourceTable(
            $bucketDatabaseName,
            'sharedTable',
            $sourceBqClient
        );

        // share the bucket
        $publicPart = (array) json_decode(
            $sourceProjectResponse->getProjectUserName(),
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
            ->setSourceProjectReadOnlyRoleName($sourceProjectResponse->getProjectReadOnlyRoleName());

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
            $targetProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR
        );
        /** @var string $targetProjectId */
        $targetProjectId = $publicPart['project_id'];
        $handler = new LinkBucketHandler($this->clientManager);
        $command = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($targetProjectId)
            ->setTargetBucketId($linkedBucketSchemaName)
            ->setSourceShareRoleName($listing); // listing
        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        return [
            $targetProjectCredentials,
            $targetProjectResponse,
            $linkedBucketSchemaName,
            'sharedTable',
            function () use ($linkedBucketSchemaName, $targetProjectCredentials): void {
                $unlinkHandler = new UnLinkBucketHandler($this->clientManager);
                $command = (new UnLinkBucketCommand())
                    ->setBucketObjectName($linkedBucketSchemaName);

                $unlinkHandler(
                    $targetProjectCredentials,
                    $command,
                    [],
                    new RuntimeOptions(['runId' => $this->testRunId]),
                );
            },
        ];
    }
}
