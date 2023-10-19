<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Info;

use Keboola\StorageDriver\BigQuery\Handler\Info\ObjectInfoHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\ObjectNotFoundException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;

class ObjectInfoErrorTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->bucketResponse = $this->createTestBucket($this->projects[0][0], $this->projects[0][2]);
        $this->projectCredentials = $this->projects[0][0];
    }

    public function testInfoSchemaNotExists(): void
    {
        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setLogger($this->log);
        $command = new ObjectInfoCommand();
        // expect database
        $command->setExpectedObjectType(ObjectType::SCHEMA);
        $command->setPath(ProtobufHelper::arrayToRepeatedString(['iAmNotExist']));
        $this->expectException(ObjectNotFoundException::class);
        $this->expectExceptionMessage('Object "iAmNotExist" not found.');
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    public function testInfoTableSchemaNotExists(): void
    {
        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setLogger($this->log);
        $command = new ObjectInfoCommand();
        // expect database
        $command->setExpectedObjectType(ObjectType::TABLE);
        $command->setPath(ProtobufHelper::arrayToRepeatedString(['databaseNotExists', 'iAmNotExist']));
        $this->expectException(ObjectNotFoundException::class);
        $this->expectExceptionMessage('Object "databaseNotExists" not found.');
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    public function testInfoTableNotExists(): void
    {
        $handler = new ObjectInfoHandler($this->clientManager);
        $handler->setLogger($this->log);
        $command = new ObjectInfoCommand();
        // expect database
        $command->setExpectedObjectType(ObjectType::TABLE);
        $command->setPath(ProtobufHelper::arrayToRepeatedString([
            $this->bucketResponse->getCreateBucketObjectName(),
            'iAmNotExist',
        ]));
        $this->expectException(ObjectNotFoundException::class);
        $this->expectExceptionMessage('Object "iAmNotExist" not found.');
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    public function testInfoViewNotExists(): void
    {
        $this->markTestSkipped('View info is TODO');
        //$handler = new ObjectInfoHandler($this->sessionManager);
        //$command = new ObjectInfoCommand();
        //// expect database
        //$command->setExpectedObjectType(ObjectType::VIEW);
        //$command->setPath(ProtobufHelper::arrayToRepeatedString(['databaseNotExists', 'iAmNotExist']));
        //$this->expectException(ObjectNotFoundException::class);
        //$this->expectExceptionMessage('Object "iAmNotExist" not found.');
        //$handler(
        //    $this->projectCredentials,
        //    $command,
        //    []
        //);
    }
}
