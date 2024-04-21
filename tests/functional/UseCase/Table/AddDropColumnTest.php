<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\AddColumnHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\DropColumnHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\AddColumnCommand;
use Keboola\StorageDriver\Command\Table\DropColumnCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

class AddDropColumnTest extends BaseCase
{
    protected CreateBucketResponse $bucketResponse;

    private GenericBackendCredentials $projectCredentials;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];

        $this->bucketResponse = $this->createTestBucket($this->projects[0][0], $this->projects[0][2]);
    }

    public function testAddColumn(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        $tableName = $this->getTestHash() . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        $tableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $tableName,
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
            $tableDef->getSchemaName(),
            $tableDef->getTableName(),
            $tableDef->getColumnsDefinitions(),
            $tableDef->getPrimaryKeysNames(),
        );
        $bqClient->executeQuery($bqClient->query($sql));

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $command = (new AddColumnCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumnDefinition(
                (new TableColumnShared())
                    ->setName('newCol')
                    ->setType(Bigquery::TYPE_BIGINT),
            );
        $handler = new AddColumnHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertNotNull($response->getTableInfo());

        $bqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->projectCredentials,
        );

        $tableRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $tableName);
        $this->assertEquals(['col1', 'col2', 'col3', 'newCol'], $tableRef->getColumnsNames());
        foreach ($tableRef->getColumnsDefinitions() as $colDef) {
            /** @var ColumnInterface $colDef */
            if ($colDef->getColumnName() === 'newCol') {
                $this->assertEquals(BaseType::INTEGER, $colDef->getColumnDefinition()->getBasetype());
                break;
            }
        }
    }

    public function testDropColumn(): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        $tableName = $this->getTestHash() . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        $tableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $tableName,
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
            $tableDef->getSchemaName(),
            $tableDef->getTableName(),
            $tableDef->getColumnsDefinitions(),
            $tableDef->getPrimaryKeysNames(),
        );
        $bqClient->executeQuery($bqClient->query($sql));

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;

        $command = (new DropColumnCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumnName('col2');
        $handler = new DropColumnHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        $tableRef = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $tableName);
        $this->assertEquals(['col1', 'col3'], $tableRef->getColumnsNames());
    }
}
