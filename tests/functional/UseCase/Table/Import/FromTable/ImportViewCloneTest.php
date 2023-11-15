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

/**
 * @group Import
 */
class ImportViewCloneTest extends BaseCase
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
        $bucketResponse = $this->createTestBucket($this->projects[0][0], $this->projects[0][2]);
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projects[0][0]);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_Test_table';
        $this->createSourceTable(
            $bucketDatabaseName,
            $sourceTableName,
            $bqClient
        );
        // create also destination so table exists
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
                ->setCreateMode(ImportOptions\CreateMode::REPLACE)
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
            iterator_to_array($response->getImportedColumns())
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
            3
        );
    }

    /**
     * @dataProvider importTypeProvider
     * @param ImportOptions\ImportType::* $importType
     */
    public function testImportAsView(int $importType): void
    {
        // create resources
        $bucketResponse = $this->createTestBucket($this->projects[0][0], $this->projects[0][2]);
        $destinationTableName = $this->getTestHash() . '_Test_table_final';
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projects[0][0]);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_Test_table';
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
            iterator_to_array($response->getImportedColumns())
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
            3
        );

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
            $this->projects[1][2]
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
            iterator_to_array($response->getImportedColumns())
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
            $destinationTableName
        );
        // this will be also 0 for view but will match result from reflection
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        $this->assertViewOrTableRowsCount(
            $bqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            $destinationTableName,
            3
        );

        // check table read as WS user
        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $workspaceCredentials);
        $ref = new BigqueryTableReflection(
            $wsBqClient,
            $workspaceResponse->getWorkspaceObjectName(),
            $destinationTableName
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
            3
        );

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
            implode(',', $insert)
        )));
    }

    /**
     * @return array{GenericBackendCredentials, CreateProjectResponse,string,string,callable}
     */
    private function createLinkedBucketWithTable(): array
    {
        $bucketResponse = $this->createTestBucket($this->projects[0][0], $this->projects[0][2]);
        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $sourceBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projects[0][0]);
        $linkedBucketSchemaName = $bucketDatabaseName . '_LINKED';

        // create source table to be shared
        $this->createSourceTable(
            $bucketDatabaseName,
            'sharedTable',
            $sourceBqClient
        );

        // share the bucket
        $publicPart = (array) json_decode(
            $this->projects[0][1]->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR
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
            JSON_THROW_ON_ERROR
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
        int $expectedRowsCount
    ): void {
        $result = $bqClient->runQuery($bqClient->query(
            sprintf(
                'SELECT * FROM %s.%s',
                BigqueryQuote::quoteSingleIdentifier($datasetName),
                BigqueryQuote::quoteSingleIdentifier($tableName)
            )
        ));
        $this->assertCount($expectedRowsCount, $result);
    }
}
