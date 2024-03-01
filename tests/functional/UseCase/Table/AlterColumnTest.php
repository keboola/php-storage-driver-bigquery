<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Generator;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Common;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\AlterColumnHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\LogMessage;
use Keboola\StorageDriver\Command\Common\LogMessage\Level;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Table\AlterColumnCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;

class AlterColumnTest extends BaseCase
{
    protected CreateBucketResponse $bucketResponse;

    private string $tableName;

    private GenericBackendCredentials $projectCredentials;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];

        $this->bucketResponse = $this->createTestBucket($this->projects[0][0], $this->projects[0][2]);
        $this->tableName = $this->getTestHash() . '_Test_table';
    }

    public function nullabilityProvider(): Generator
    {
        yield 'required -> required' => [
            'col2Required',
            false,
            false,
            [],
            [],
        ];

        yield 'required -> nullable' => [
            'col2Required',
            true,
            true,
            [Common::KBC_METADATA_KEY_NULLABLE],
            [],
        ];

        yield 'nullable -> nullable' => [
            'col1Nullable',
            true,
            true,
            [],
            ['"KBC.datatype.nullable": Cannot DROP NOT NULL constraint from column col1Nullable which does not have a NOT NULL constraint.'],
        ];

        yield 'nullable -> required' => [
            'col1Nullable',
            false,
            true,
            [],
            [],
        ];
    }

    /**
     * @dataProvider nullabilityProvider
     * @param string[] $expectedSuccessLog
     * @param string[] $expectedErrorLog
     */
    public function testNullability(
        string $columnName,
        bool $setNullable,
        bool $expectedNullable,
        array $expectedSuccessLog,
        array $expectedErrorLog,
    ): void {
        $datasetName = $this->bucketResponse->getCreateBucketObjectName();
        $this->createRefTable($datasetName, $this->tableName);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $datasetName;

        $fields = new RepeatedField(GPBType::STRING);
        $fields[] = Common::KBC_METADATA_KEY_NULLABLE;

        $command = (new AlterColumnCommand())
            ->setPath($path)
            ->setTableName($this->tableName)
            ->setAttributesToUpdate($fields)
            ->setDesiredDefiniton(
                (new TableColumnShared())
                    ->setType(Bigquery::TYPE_INT64)
                    ->setName($columnName)
                    ->setNullable($setNullable),
            );
        $handler = new AlterColumnHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $infoLogs = array_map(
            fn(LogMessage $log,) => $log->getMessage(),
            $this->getLogsOfLevel($handler, Level::Informational),
        );
        $errorLogs = array_map(
            fn(LogMessage $log, ) => $log->getMessage(),
            $this->getLogsOfLevel($handler, Level::Error)
        );

        $this->assertEqualsArrays($expectedSuccessLog, $infoLogs);
        $this->assertEqualsArrays($expectedErrorLog, $errorLogs);

        $checkedColumn = $this->extractColumnFromResponse($response, $columnName);
        $this->assertSame($expectedNullable, $checkedColumn->getNullable());
    }

    public function testDefault(): void
    {
        $datasetName = $this->bucketResponse->getCreateBucketObjectName();
        $this->createRefTable($datasetName, $this->tableName);

        // set 1234 on int
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $datasetName;

        $fields = new RepeatedField(GPBType::STRING);
        $fields[] = Common::KBC_METADATA_KEY_DEFAULT;

        $command = (new AlterColumnCommand())
            ->setPath($path)
            ->setTableName($this->tableName)
            ->setAttributesToUpdate($fields)
            ->setDesiredDefiniton(
                (new TableColumnShared())
                    ->setType(Bigquery::TYPE_INT64)
                    ->setName('col2Required')
                    ->setDefault('1234'),
            );
        $handler = new AlterColumnHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertCount(1, $this->getLogsOfLevel($handler, Level::Informational));
        $this->assertCount(0, $this->getLogsOfLevel($handler, Level::Error));

        $checkedColumn = $this->extractColumnFromResponse($response, 'col2Required');
        $this->assertSame('1234', $checkedColumn->getDefault());

        // TODO
        // set helloworld on int
//        $path = new RepeatedField(GPBType::STRING);
//        $path[] = $datasetName;
//
//        $fields = new RepeatedField(GPBType::STRING);
//        $fields[] = Common::KBC_METADATA_KEY_DEFAULT;
//
//        $command = (new AlterColumnCommand())
//            ->setPath($path)
//            ->setTableName($this->tableName)
//            ->setAttributesToUpdate($fields)
//            ->setDesiredDefiniton(
//                (new TableColumnShared())
//                    ->setType(Bigquery::TYPE_INT64)
//                    ->setName('col2Required')
//                    ->setDefault('helloworld'),
//            );
//        $handler = new AlterColumnHandler($this->clientManager);
//        $handler->setInternalLogger($this->log);
//
//        /** @var ObjectInfoResponse $response */
//        $response = $handler(
//            $this->projectCredentials,
//            $command,
//            [],
//            new RuntimeOptions(['runId' => $this->testRunId]),
//        );
//
//        $this->assertCount(0, $this->getLogsOfLevel($handler, Level::Informational));
//        $this->assertCount(1, $this->getLogsOfLevel($handler, Level::Error));
//
//        $checkedColumn = $this->extractColumnFromResponse($response, 'col2Required');
//        $this->assertSame(null, $checkedColumn->getDefault());

        // set helloworld on string
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $datasetName;

        $fields = new RepeatedField(GPBType::STRING);
        $fields[] = Common::KBC_METADATA_KEY_DEFAULT;

        $command = (new AlterColumnCommand())
            ->setPath($path)
            ->setTableName($this->tableName)
            ->setAttributesToUpdate($fields)
            ->setDesiredDefiniton(
                (new TableColumnShared())
                    ->setType(Bigquery::TYPE_STRING)
                    ->setName('col3String')
                    ->setDefault('helloworld'),
            );
        $handler = new AlterColumnHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertCount(1, $this->getLogsOfLevel($handler, Level::Informational));
        $this->assertCount(0, $this->getLogsOfLevel($handler, Level::Error));

        $checkedColumn = $this->extractColumnFromResponse($response, 'col3String');
        $this->assertSame('\'helloworld\'', $checkedColumn->getDefault());
    }

    public function testLength(): void
    {
    }

    protected function createRefTable(string $bucketDatabaseName, string $tableName): void
    {
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        $tableDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $tableName,
            false,
            new ColumnCollection([
                new BigqueryColumn(
                    'col1Nullable',
                    new Bigquery(Bigquery::TYPE_INT64, ['nullable' => true]),
                ),
                new BigqueryColumn(
                    'col2Required',
                    new Bigquery(Bigquery::TYPE_INT64, ['nullable' => false]),
                ),
                new BigqueryColumn(
                    'col3String',
                    new Bigquery(Bigquery::TYPE_STRING, ['nullable' => false]),
                ),
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
        $bqClient->runQuery($bqClient->query($sql));
    }
}
