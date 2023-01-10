<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use DateTimeImmutable;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableFromTimeTravelHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\TableInfo\TableColumn;
use Keboola\StorageDriver\Command\Table\CreateTableFromTimeTravelCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\ObjectNotFoundException;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

class CreateTableFromTimeTravelTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        $this->bucketResponse = $this->createTestBucket($projectCredentials);
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testCreateTableFromTimestamp(): void
    {
        $sourceTableName = md5($this->getName()) . '_Test_table';
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // create tables
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $this->initTestTableAndImportBaseData($bqClient, $bucketDatasetName, $sourceTableName);

        sleep(1);
        $dateTimeAfterImport = date(DATE_ATOM);

        foreach ([['5', '5', '6'], ['7', '7', '7'], ['8', '8', '8'], ['9', '8', '9']] as $i) {
            $bqClient->runQuery($bqClient->query(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
                BigqueryQuote::quoteSingleIdentifier($sourceTableName),
                implode(',', $i)
            )));
        }

        $cmd = new CreateTableFromTimeTravelCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $cmd->setSource(
            (new CreateTableFromTimeTravelCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
        );

        $destinationTableName = md5($this->getName()) . '_createdFromTimeTravel';
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );

        /** @var int $dateTimeAfterImportTimestamp */
        $dateTimeAfterImportTimestamp = strtotime($dateTimeAfterImport);
        $cmd->setTimestamp($dateTimeAfterImportTimestamp);

        $handler = new CreateTableFromTimeTravelHandler($this->clientManager);

        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            []
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertNotNull($response->getTableInfo());

        $tableInfo = $response->getTableInfo();

        $this->assertSame(4, $tableInfo->getRowsCount());
        $this->assertGreaterThan(0, $tableInfo->getSizeBytes());

        /** @var TableColumn[] $columns */
        $columns = iterator_to_array($tableInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableColumn $col) => $col->getName(),
            $columns
        );
        $this->assertSame(
            ['col1', 'col2', 'col3'],
            $columnsNames
        );

        $sourceRef = new BigqueryTableReflection($bqClient, $bucketDatasetName, $sourceTableName);
        $this->assertSame(8, $sourceRef->getRowsCount());

        $destinationRef = new BigqueryTableReflection($bqClient, $bucketDatasetName, $destinationTableName);
        $this->assertSame(4, $destinationRef->getRowsCount());
    }

    public function testCreateTableFromTimestampOfAlteredTable(): void
    {
        $sourceTableName = md5($this->getName()) . '_Test_table';
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $this->initTestTableAndImportBaseData($bqClient, $bucketDatasetName, $sourceTableName);

        sleep(1);
        $dateTimeBeforeAlter = date(DATE_ATOM);

        // add column
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getAddColumnCommand(
            $bucketDatasetName,
            $sourceTableName,
            new BigqueryColumn('newColumn', new Bigquery(
                Bigquery::TYPE_STRING
            ))
        );

        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $bqClient->runQuery($bqClient->query($sql));

        $cmd = new CreateTableFromTimeTravelCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $cmd->setSource(
            (new CreateTableFromTimeTravelCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
        );

        $destinationTableName = md5($this->getName()) . '_createdFromTimeTravel';
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );

        /** @var int $dateTimeBeforeAlterTimestamp */
        $dateTimeBeforeAlterTimestamp = strtotime($dateTimeBeforeAlter);
        $cmd->setTimestamp($dateTimeBeforeAlterTimestamp);

        $handler = new CreateTableFromTimeTravelHandler($this->clientManager);

        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            []
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertNotNull($response->getTableInfo());

        $tableInfo = $response->getTableInfo();

        $this->assertSame(4, $tableInfo->getRowsCount());
        $this->assertGreaterThan(0, $tableInfo->getSizeBytes());

        /** @var TableColumn[] $columns */
        $columns = iterator_to_array($tableInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableColumn $col) => $col->getName(),
            $columns
        );
        $this->assertSame(
            ['col1', 'col2', 'col3'],
            $columnsNames
        );

        $sourceRef = new BigqueryTableReflection($bqClient, $bucketDatasetName, $sourceTableName);
        $this->assertSame(['col1', 'col2', 'col3', 'newColumn'], $sourceRef->getColumnsNames());

        $destinationRef = new BigqueryTableReflection($bqClient, $bucketDatasetName, $destinationTableName);
        $this->assertSame(['col1', 'col2', 'col3'], $destinationRef->getColumnsNames());
    }

    public function testInvalidCreateTableFromTimestamp(): void
    {
        $sourceTableName = md5($this->getName()) . '_Test_table';
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $this->initTestTableAndImportBaseData($bqClient, $bucketDatasetName, $sourceTableName);

        // test create table from non-existing table
        $cmd = new CreateTableFromTimeTravelCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $cmd->setSource(
            (new CreateTableFromTimeTravelCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName('table_should_never_be_created')
        );

        $destinationTableName = md5($this->getName()) . '_createdFromTimeTravel';
        $destPath = new RepeatedField(GPBType::STRING);
        $destPath[] = $bucketDatasetName;
        $cmd->setDestination(
            (new Table())
                ->setPath($destPath)
                ->setTableName($destinationTableName)
        );

        /** @var int $timestamp */
        $timestamp = strtotime(date(DATE_ATOM));
        $cmd->setTimestamp($timestamp);

        $handler = new CreateTableFromTimeTravelHandler($this->clientManager);

        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                []
            );
            $this->fail('Should fail: Table should never be created.');
        } catch (ObjectNotFoundException $e) {
            $this->assertSame('Object "table_should_never_be_created" not found.', $e->getMessage());
        }

        // test create table according timestamp when it didn't exist yet
        $cmd = new CreateTableFromTimeTravelCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $cmd->setSource(
            (new CreateTableFromTimeTravelCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
        );

        $destinationTableName = md5($this->getName()) . '_createdFromTimeTravel';
        $destPath = new RepeatedField(GPBType::STRING);
        $destPath[] = $bucketDatasetName;
        $cmd->setDestination(
            (new Table())
                ->setPath($destPath)
                ->setTableName($destinationTableName)
        );

        $datetime = new DateTimeImmutable('1989-08-31 06:00:00');
        $cmd->setTimestamp($datetime->getTimestamp());

        $handler = new CreateTableFromTimeTravelHandler($this->clientManager);

        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                []
            );
            $this->fail('Should fail: Table should not exist in this point of time.');
        } catch (BadRequestException $e) {
            $this->assertSame(400, $e->getCode());
            $tableInfo = $bqClient->dataset($bucketDatasetName)->table($sourceTableName)->info();
            $this->assertStringContainsString('Cannot read before ' . $tableInfo['creationTime'], $e->getMessage());
        }

        $dateTimeBeforeDelete = date(DATE_ATOM);

        $bqClient->dataset($bucketDatasetName)->table($sourceTableName)->delete();

        $cmd = new CreateTableFromTimeTravelCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $cmd->setSource(
            (new CreateTableFromTimeTravelCommand\SourceTableMapping())
                ->setPath($path)
                ->setTableName($sourceTableName)
        );

        $destinationTableName = md5($this->getName()) . '_createdFromTimeTravel';
        $destPath = new RepeatedField(GPBType::STRING);
        $destPath[] = $bucketDatasetName;
        $cmd->setDestination(
            (new Table())
                ->setPath($destPath)
                ->setTableName($destinationTableName)
        );

        /** @var int $timestamp */
        $timestamp = strtotime($dateTimeBeforeDelete);
        $cmd->setTimestamp($timestamp);

        $handler = new CreateTableFromTimeTravelHandler($this->clientManager);

        $this->expectException(ObjectNotFoundException::class);
        $handler(
            $this->projectCredentials,
            $cmd,
            []
        );
    }

    private function initTestTableAndImportBaseData(
        BigQueryClient $bqClient,
        string $bucketDatasetName,
        string $sourceTableName
    ): void {
        $tableSourceDef = new BigqueryTableDefinition(
            $bucketDatasetName,
            $sourceTableName,
            false,
            new ColumnCollection([
                new BigqueryColumn('col1', new Bigquery(
                    Bigquery::TYPE_INT,
                    []
                )),
                new BigqueryColumn('col2', new Bigquery(
                    Bigquery::TYPE_BIGINT,
                    []
                )),
                new BigqueryColumn('col3', new Bigquery(
                    Bigquery::TYPE_INT,
                    []
                )),
            ]),
            ['col1']
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableSourceDef->getSchemaName(),
            $tableSourceDef->getTableName(),
            $tableSourceDef->getColumnsDefinitions(),
            [], //<-- dont create primary keys allow duplicates
        );
        $bqClient->runQuery($bqClient->query($sql));
        foreach ([['1', '1', '3'], ['2', '2', '2'], ['3', '2', '3'], ['4', '4', '4']] as $i) {
            $bqClient->runQuery($bqClient->query(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
                BigqueryQuote::quoteSingleIdentifier($sourceTableName),
                implode(',', $i)
            )));
        }
    }
}
