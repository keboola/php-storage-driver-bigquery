<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Info;

use Keboola\StorageDriver\BigQuery\Handler\Info\ObjectInfoHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Info\ObjectInfo;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\TableInfo\TableColumn;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Traversable;

class ObjectInfoTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    private CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];
        $this->projectResponse = $this->projects[0][1];

        // create bucket
        $this->bucketResponse = $this->createTestBucket($this->projects[0][0], $this->projects[0][2]);

        $this->createTestTable(
            $this->projectCredentials,
            $this->bucketResponse->getCreateBucketObjectName(),
            $this->getTestHash()
        );
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE VIEW %s.`bucket_view1` AS '
            . 'SELECT * FROM %s.%s;',
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($this->bucketResponse->getCreateBucketObjectName()),
            BigqueryQuote::quoteSingleIdentifier($this->getTestHash()),
        )));
    }

    public function testInfoDatabase(): void
    {
        // create workspace
        [
            ,
            $workspaceResponse,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projects[0][1], $this->projects[0][2]);
        $this->createTestTable(
            $this->projectCredentials,
            $workspaceResponse->getWorkspaceObjectName(),
            'ws_table1'
        );

        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        // expect database
        $command->setExpectedObjectType(ObjectType::DATABASE);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([$this->projectResponse->getProjectUserName()]));
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::DATABASE, $response->getObjectType());
        $this->assertTrue($response->hasDatabaseInfo());
        $this->assertFalse($response->hasSchemaInfo());
        $this->assertFalse($response->hasTableInfo());
        $this->assertFalse($response->hasViewInfo());
        $this->assertNotNull($response->getDatabaseInfo());
        $this->assertSame(
            [$this->projectResponse->getProjectUserName()],
            ProtobufHelper::repeatedStringToArray($response->getPath())
        );
        /** @var Traversable<ObjectInfo> $objects */
        $objects = $response->getDatabaseInfo()->getObjects()->getIterator();
        $bucketObject = $this->getObjectByNameAndType(
            $objects,
            $this->bucketResponse->getCreateBucketObjectName()
        );
        $this->assertSame(ObjectType::SCHEMA, $bucketObject->getObjectType());
        $workspaceObject = $this->getObjectByNameAndType(
            $objects,
            $workspaceResponse->getWorkspaceObjectName()
        );
        $this->assertSame(ObjectType::SCHEMA, $workspaceObject->getObjectType());
    }

    public function testInfoSchema(): void
    {
        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        $command->setExpectedObjectType(ObjectType::SCHEMA);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([$this->bucketResponse->getCreateBucketObjectName()]));
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::SCHEMA, $response->getObjectType());
        $this->assertSame(
            [$this->bucketResponse->getCreateBucketObjectName()],
            ProtobufHelper::repeatedStringToArray($response->getPath())
        );
        $this->assertFalse($response->hasDatabaseInfo());
        $this->assertTrue($response->hasSchemaInfo());
        $this->assertFalse($response->hasTableInfo());
        $this->assertFalse($response->hasViewInfo());
        $this->assertNotNull($response->getSchemaInfo());
        /** @var Traversable<ObjectInfo> $objects */
        $objects = $response->getSchemaInfo()->getObjects()->getIterator();
        $table = $this->getObjectByNameAndType(
            $objects,
            $this->getTestHash()
        );
        $this->assertSame(ObjectType::TABLE, $table->getObjectType());
        $view = $this->getObjectByNameAndType(
            $objects,
            'bucket_view1'
        );
        $this->assertSame(ObjectType::VIEW, $view->getObjectType());
    }

    public function testInfoTable(): void
    {
        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        $command->setExpectedObjectType(ObjectType::TABLE);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([
            $this->bucketResponse->getCreateBucketObjectName(),
            $this->getTestHash(),
        ]));
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertSame(
            [
                $this->bucketResponse->getCreateBucketObjectName(),
                $this->getTestHash(),
            ],
            ProtobufHelper::repeatedStringToArray($response->getPath())
        );
        $this->assertFalse($response->hasDatabaseInfo());
        $this->assertFalse($response->hasSchemaInfo());
        $this->assertTrue($response->hasTableInfo());
        $this->assertFalse($response->hasViewInfo());

        $tableInfo = $response->getTableInfo();
        $this->assertNotNull($tableInfo);
        $this->assertSame($this->getTestHash(), $tableInfo->getTableName());
        $this->assertSame(
            [$this->bucketResponse->getCreateBucketObjectName()],
            ProtobufHelper::repeatedStringToArray($tableInfo->getPath())
        );
        $this->assertSame(
            [],
            ProtobufHelper::repeatedStringToArray($tableInfo->getPrimaryKeysNames())
        );
        /** @var TableColumn[] $columns */
        $columns = iterator_to_array($tableInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableColumn $col) => $col->getName(),
            $columns
        );
        $this->assertSame(['id', 'name', 'large'], $columnsNames);
    }

    public function testInfoView(): void
    {
        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = new ObjectInfoCommand();
        $command->setExpectedObjectType(ObjectType::VIEW);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([
            $this->bucketResponse->getCreateBucketObjectName(),
            'bucket_view1',
        ]));
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::VIEW, $response->getObjectType());
        $this->assertSame(
            [
                $this->bucketResponse->getCreateBucketObjectName(),
                'bucket_view1',
            ],
            ProtobufHelper::repeatedStringToArray($response->getPath())
        );
        $this->assertFalse($response->hasDatabaseInfo());
        $this->assertFalse($response->hasSchemaInfo());
        $this->assertFalse($response->hasTableInfo());
        $this->assertTrue($response->hasViewInfo());

        $viewInfo = $response->getViewInfo();

        $this->assertNotNull($viewInfo);
        $this->assertSame('bucket_view1', $viewInfo->getViewName());
        $this->assertSame(
            [$this->bucketResponse->getCreateBucketObjectName()],
            ProtobufHelper::repeatedStringToArray($viewInfo->getPath())
        );
        /** @var TableColumn[] $columns */
        $columns = iterator_to_array($viewInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableColumn $col) => $col->getName(),
            $columns
        );
        $this->assertSame(['id', 'name', 'large'], $columnsNames);
    }

    /** @param Traversable<ObjectInfo> $objects */
    private function getObjectByNameAndType(Traversable $objects, string $expectedName): ObjectInfo
    {
        foreach ($objects as $objectInfo) {
            if ($objectInfo->getObjectName() === $expectedName) {
                return $objectInfo;
            }
        }
        $this->fail(sprintf('Expected object name "%s" not found.', $expectedName));
    }
}
