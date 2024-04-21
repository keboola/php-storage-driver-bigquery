<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Generator;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Common;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\AlterColumnException;
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

    public function invalidDefaultProvider(): Generator
    {
        yield 'set helloworld on int' => [
            'col2Required',
            Bigquery::TYPE_INT64,
            'helloworld',
            'Invalid default value for column "col2Required". Expected numeric value, got "helloworld".',
        ];
        yield 'set helloworld on boolean' => [
            'col4Bool',
            Bigquery::TYPE_BOOL,
            'helloworld',
            'Invalid default value for column "col4Bool". Allowed values are true, false, 0, 1, got "helloworld".',
        ];
    }

    public function lengthProvider(): Generator
    {
        yield '200 -> 300' => [
            'col5WithLength',
            '300',
            '300',
            [Common::KBC_METADATA_KEY_LENGTH],
            '',
        ];
        yield '200 -> 100' => [
            'col5WithLength',
            '100',
            '200',
            [],
            'Narrowing type parameters is not compatible',
        ];
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
            // phpcs:ignore
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

    public function defaultProvider(): Generator
    {
        yield 'set 1234 on int' => [
            'col1Nullable',
            Bigquery::TYPE_INT64,
            '1234',
            '1234',
            [Common::KBC_METADATA_KEY_DEFAULT],
            [],
        ];
        yield 'set helloworld on string' => [
            'col3String',
            Bigquery::TYPE_STRING,
            'helloworld',
            '\'helloworld\'',
            [Common::KBC_METADATA_KEY_DEFAULT],
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

        // check logs for success and error
        $infoLogs = array_map(
            fn(LogMessage $log) => $log->getMessage(),
            $this->getLogsOfLevel($handler, Level::Informational),
        );
        $errorLogs = array_map(
            fn(LogMessage $log) => $log->getMessage(),
            $this->getLogsOfLevel($handler, Level::Error),
        );

        $this->assertEqualsArrays($expectedSuccessLog, $infoLogs);
        $this->assertEqualsArrays($expectedErrorLog, $errorLogs);

        $checkedColumn = $this->extractColumnFromResponse($response, $columnName);
        $this->assertSame($expectedNullable, $checkedColumn->getNullable());
    }

    /**
     * @dataProvider defaultProvider
     * @param string[] $expectedSuccessLog
     * @param string[] $expectedErrorLog
     */
    public function testDefault(
        string $columnName,
        string $type,
        string $setDefault,
        string $expectedDefault,
        array $expectedSuccessLog,
        array $expectedErrorLog,
    ): void {
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
                    ->setType($type)
                    ->setName($columnName)
                    ->setDefault($setDefault),
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
            fn(LogMessage $log) => $log->getMessage(),
            $this->getLogsOfLevel($handler, Level::Informational),
        );
        $errorLogs = array_map(
            fn(LogMessage $log) => $log->getMessage(),
            $this->getLogsOfLevel($handler, Level::Error),
        );

        $this->assertEqualsArrays($expectedSuccessLog, $infoLogs);
        $this->assertEqualsArrays($expectedErrorLog, $errorLogs);

        $checkedColumn = $this->extractColumnFromResponse($response, $columnName);
        $this->assertSame($expectedDefault, $checkedColumn->getDefault());
    }

    /**
     * usecases which fail even on input validation and won't even make it to execution
     *
     * @dataProvider invalidDefaultProvider
     */
    public function testInvalidDefaults(
        string $columnName,
        string $type,
        string $setDefault,
        string $expectedErrorMessage,
    ): void {
        $datasetName = $this->bucketResponse->getCreateBucketObjectName();
        $this->createRefTable($datasetName, $this->tableName);

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
                    ->setType($type)
                    ->setName($columnName)
                    ->setDefault($setDefault),
            );
        $handler = new AlterColumnHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        try {
            $handler(
                $this->projectCredentials,
                $command,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('should fail');
        } catch (AlterColumnException $e) {
            $this->assertEquals($expectedErrorMessage, $e->getMessage());
        }
    }

    public function testInvalidOperation(): void
    {
        $datasetName = $this->bucketResponse->getCreateBucketObjectName();
        $this->createRefTable($datasetName, $this->tableName);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $datasetName;

        $fields = new RepeatedField(GPBType::STRING);
        $fields[] = 'not-existing';

        $command = (new AlterColumnCommand())
            ->setPath($path)
            ->setTableName($this->tableName)
            ->setAttributesToUpdate($fields)
            ->setDesiredDefiniton(
                (new TableColumnShared())
                    ->setType(Bigquery::TYPE_INT64)
                    ->setName('col1Num'),
            );
        $handler = new AlterColumnHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        try {
            $handler(
                $this->projectCredentials,
                $command,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('should fail');
        } catch (AlterColumnException $e) {
            $this->assertEquals('Unknown metadata key to "not-existing" to update.', $e->getMessage());
        }
    }

    /**
     * @dataProvider lengthProvider
     * @param string[] $expectedSuccessLog
     */
    public function testLength(
        string $columnName,
        string $setLength,
        string $expectedLength,
        array $expectedSuccessLog,
        string $expectedErrorLog,
    ): void {
        $datasetName = $this->bucketResponse->getCreateBucketObjectName();
        $this->createRefTable($datasetName, $this->tableName);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $datasetName;

        $fields = new RepeatedField(GPBType::STRING);
        $fields[] = Common::KBC_METADATA_KEY_LENGTH;

        $command = (new AlterColumnCommand())
            ->setPath($path)
            ->setTableName($this->tableName)
            ->setAttributesToUpdate($fields)
            ->setDesiredDefiniton(
                (new TableColumnShared())
                    ->setType(Bigquery::TYPE_STRING)
                    ->setName($columnName)
                    ->setLength($setLength),
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
            fn(LogMessage $log) => $log->getMessage(),
            $this->getLogsOfLevel($handler, Level::Informational),
        );
        $errorLogs = array_map(
            fn(LogMessage $log) => $log->getMessage(),
            $this->getLogsOfLevel($handler, Level::Error),
        );

        $this->assertEqualsArrays($expectedSuccessLog, $infoLogs);
        // error message contains path to resource (generated dataset name)
        if ($expectedErrorLog) {
            $this->assertCount(1, $errorLogs);
            $this->assertStringContainsString($expectedErrorLog, $errorLogs[0]);
        }

        $checkedColumn = $this->extractColumnFromResponse($response, $columnName);
        $this->assertSame($expectedLength, $checkedColumn->getLength());
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
                new BigqueryColumn(
                    'col4Bool',
                    new Bigquery(Bigquery::TYPE_BOOL, ['nullable' => false]),
                ),
                new BigqueryColumn(
                    'col5WithLength',
                    new Bigquery(Bigquery::TYPE_STRING, ['length' => '200']),
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
        $bqClient->executeQuery($bqClient->query($sql));
    }
}
