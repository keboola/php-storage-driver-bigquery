<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\ExecuteQuery;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\ExecuteQuery\ExecuteQueryHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\ExecuteQuery\ExecuteQueryCommand;
use Keboola\StorageDriver\Command\ExecuteQuery\ExecuteQueryResponse;
use Keboola\StorageDriver\Command\ExecuteQuery\ExecuteQueryResponse\Status;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use RuntimeException;

class ExecuteQueryTest extends BaseCase
{
    private GenericBackendCredentials $projectCredentials;

    private string $workspaceName;

    private string $workspaceUserName;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];

        // create workspace
        [
            ,
            $workspaceResponse,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projects[0][1]);

        $this->workspaceName = $workspaceResponse->getWorkspaceObjectName();
        $this->workspaceUserName = $workspaceResponse->getWorkspaceUserName();
        $credentialsArr = (array) json_decode(
            $workspaceResponse->getWorkspaceUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        if (!isset($credentialsArr['client_email']) || !is_string($credentialsArr['client_email'])) {
            throw new RuntimeException('Workspace user name does not contain client_email.');
        }
        $this->workspaceUserName = $credentialsArr['client_email'];
    }

    public function commandProvider(): Generator
    {
        yield 'restricted' => [
            function (self $that): ExecuteQueryCommand {
                return new ExecuteQueryCommand([
                    'pathRestriction' => ProtobufHelper::arrayToRepeatedString([$that->workspaceName]),
                    'bigQueryServiceAccount' => new ExecuteQueryCommand\BigQueryServiceAccount([
                        'serviceAccountEmail' => $that->workspaceUserName,
                        'projectId' => $that->getProjectIdFromCredentials($that->projectCredentials),
                    ]),
                ]);
            },
        ];

        yield 'unrestricted' => [
            function (self $that): ExecuteQueryCommand {
                return new ExecuteQueryCommand([
                    'pathRestriction' => ProtobufHelper::arrayToRepeatedString([$that->workspaceName]),
                ]);
            },
        ];
    }

    /**
     * @dataProvider commandProvider
     * @param callable(self):ExecuteQueryCommand $command
     */
    public function testExecuteQuerySimpleSelect(callable $command): void
    {
        $query = 'SELECT 1 AS col1, "test" AS col2';
        $command = $command($this)->setQuery($query);

        $handler = (new ExecuteQueryHandler($this->clientManager));
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ExecuteQueryResponse::class, $response);
        $this->assertEquals(Status::Success, $response->getStatus());
        $this->assertNotNull($response->getData());
        $this->assertSame(['col1', 'col2'], ProtobufHelper::repeatedStringToArray($response->getData()->getColumns()));
        $this->assertCount(1, $response->getData()->getRows());
        $rows = $this->getRows($response);
        $this->assertEquals('[{"col1":"1","col2":"test"}]', json_encode($rows));
        $this->assertStringContainsString('successfully', $response->getMessage());
    }

    /**
     * @dataProvider commandProvider
     * @param callable(self):ExecuteQueryCommand $command
     */
    public function testSelectAsterisk(callable $command): void
    {
        $this->createTable(
            $this->projectCredentials,
            $this->workspaceName,
            'test_table',
            [
                'columns' => [
                    'string_col' => [
                        'type' => Bigquery::TYPE_STRING,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'int_col' => [
                        'type' => Bigquery::TYPE_INTEGER,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'bignumeric_col' => [
                        'type' => Bigquery::TYPE_BIGNUMERIC,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'bytes_col' => [
                        'type' => Bigquery::TYPE_BYTES,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'date_col' => [
                        'type' => Bigquery::TYPE_DATE,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'geography_col' => [
                        'type' => Bigquery::TYPE_GEOGRAPHY,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'json_col' => [
                        'type' => Bigquery::TYPE_JSON,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'numeric_col' => [
                        'type' => Bigquery::TYPE_NUMERIC,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'time_col' => [
                        'type' => Bigquery::TYPE_TIME,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'timestamp_col' => [
                        'type' => Bigquery::TYPE_TIMESTAMP,
                        'length' => '',
                        'nullable' => false,
                    ],
                ],
            ],
        );
        $bigQuery = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bigQuery->runQuery($bigQuery->query(sprintf(
            /** @lang BigQuery */<<<SQL
                INSERT INTO %s.%s (
                    `string_col`,
                    `int_col`,
                    `bignumeric_col`,
                    `bytes_col`,
                    `date_col`,
                    `geography_col`,
                    `json_col`,
                    `numeric_col`,
                    `time_col`,
                    `timestamp_col`
                ) VALUES
                (
                    "test string",
                    42,
                    CAST("123456789.123456789" AS BIGNUMERIC),
                    b"binary data",
                    DATE "2024-03-15",
                    ST_GEOGPOINT(40.7128, -74.0060),
                    JSON '{"key": "value"}',
                    CAST(123.45 AS NUMERIC),
                    TIME "15:30:00",
                    TIMESTAMP "2024-03-15 15:30:00 UTC"
                ),
                (
                    "another string",
                    100,
                    CAST("987654321.987654321" AS BIGNUMERIC),
                    b"more binary",
                    DATE "2024-03-16",
                    ST_GEOGPOINT(51.5074, -0.1278),
                    JSON '{"array": [1,2,3]}',
                    CAST(678.90 AS NUMERIC),
                    TIME "18:45:00",
                    TIMESTAMP "2024-03-16 18:45:00 UTC"
                )
            SQL,
            BigqueryQuote::quoteSingleIdentifier($this->workspaceName),
            BigqueryQuote::quoteSingleIdentifier('test_table'),
        )));

        $query = 'SELECT * FROM test_table ORDER BY string_col ASC';
        $command = $command($this)->setQuery($query);

        $handler = (new ExecuteQueryHandler($this->clientManager));
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ExecuteQueryResponse::class, $response);
        $this->assertEquals(Status::Success, $response->getStatus());
        $this->assertNotNull($response->getData());
        $this->assertSame(
            [
                'string_col',
                'int_col',
                'bignumeric_col',
                'bytes_col',
                'date_col',
                'geography_col',
                'json_col',
                'numeric_col',
                'time_col',
                'timestamp_col',
            ],
            ProtobufHelper::repeatedStringToArray($response->getData()->getColumns()),
        );
        $this->assertCount(2, $response->getData()->getRows());
        $rows = $this->getRows($response);

        // Test first row
        $this->assertArrayHasKey(0, $rows);
        $row = $rows[0];
        $this->assertEquals('another string', $row['string_col']);
        $this->assertEquals('100', $row['int_col']);
        $this->assertEquals('987654321.987654321', $row['bignumeric_col']);
        $this->assertNotEmpty($row['bytes_col']); // Binary data will be base64 encoded
        $this->assertEquals('2024-03-16', $row['date_col']);
        $this->assertStringContainsString('POINT(51.5074 -0.1278)', $row['geography_col']);
        $this->assertEquals('{"array":[1,2,3]}', $row['json_col']);
        $this->assertEquals('678.9', $row['numeric_col']);
        $this->assertEquals('18:45:00.000000', $row['time_col']);
        $this->assertStringStartsWith('2024-03-16 18:45:00.000000', $row['timestamp_col']);

        $this->assertStringContainsString('successfully', $response->getMessage());
    }

    /**
     * @dataProvider commandProvider
     * @param callable(self):ExecuteQueryCommand $command
     */
    public function testExecuteCTAS(callable $command): void
    {
        $this->createTable(
            $this->projectCredentials,
            $this->workspaceName,
            'test_table',
            [
                'columns' => [
                    'col1' => [
                        'type' => Bigquery::TYPE_INTEGER,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'col2' => [
                        'type' => Bigquery::TYPE_STRING,
                        'length' => '10',
                        'nullable' => true,
                    ],
                ],
            ],
        );
        $bigQuery = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bigQuery->runQuery($bigQuery->query(sprintf(
            <<<SQL
                INSERT INTO %s.%s (`col1`, `col2`) VALUES
                (1, "Alice"),
                (2, "Bob"),
                (3, NULL)
            SQL,
            BigqueryQuote::quoteSingleIdentifier($this->workspaceName),
            BigqueryQuote::quoteSingleIdentifier('test_table'),
        )));

        $query = sprintf(
            'CREATE TABLE `test_table_2` AS SELECT * FROM %s.`test_table`',
            BigqueryQuote::quoteSingleIdentifier($this->workspaceName),
        );
        $command = $command($this)->setQuery($query);

        $handler = (new ExecuteQueryHandler($this->clientManager));
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ExecuteQueryResponse::class, $response);
        $this->assertEquals(Status::Success, $response->getStatus());
        $this->assertNotNull($response->getData());
        $this->assertSame(['col1', 'col2'], ProtobufHelper::repeatedStringToArray($response->getData()->getColumns()));
        $this->assertCount(0, $response->getData()->getRows());
        $this->assertStringContainsString('successfully', $response->getMessage());
        $this->assertSame('[]', json_encode($this->getRows($response)));
    }

    /**
     * @dataProvider commandProvider
     * @param callable(self):ExecuteQueryCommand $command
     */
    public function testExecuteError(callable $command): void
    {
        $query = sprintf(
        /** @lang BigQuery */            'CREATE TABLE `test_table_2` AS SELECT * FROM %s.`iDoNotExists`',
            BigqueryQuote::quoteSingleIdentifier($this->workspaceName),
        );
        $command = $command($this)->setQuery($query);

        $handler = (new ExecuteQueryHandler($this->clientManager));
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ExecuteQueryResponse::class, $response);
        $this->assertEquals(Status::Error, $response->getStatus());
        $this->assertMatchesRegularExpression(
            '/^Not found: Table [^:]+:[^\.]+\.iDoNotExists was not found in location US/',
            $response->getMessage(),
        );
    }

    /**
     * @dataProvider commandProvider
     * @param callable(self):ExecuteQueryCommand $command
     */
    public function testExecuteInsert(callable $command): void
    {
        $this->createTable(
            $this->projectCredentials,
            $this->workspaceName,
            'test_table',
            [
                'columns' => [
                    'col1' => [
                        'type' => Bigquery::TYPE_INTEGER,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'col2' => [
                        'type' => Bigquery::TYPE_STRING,
                        'length' => '10',
                        'nullable' => true,
                    ],
                ],
            ],
        );
        $query = sprintf(
            <<<SQL
                INSERT INTO %s.%s (`col1`, `col2`) VALUES
                (1, "Alice"),
                (2, "Bob"),
                (3, NULL)
            SQL,
            BigqueryQuote::quoteSingleIdentifier($this->workspaceName),
            BigqueryQuote::quoteSingleIdentifier('test_table'),
        );
        $command = $command($this)->setQuery($query);

        $handler = (new ExecuteQueryHandler($this->clientManager));
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ExecuteQueryResponse::class, $response);
        $this->assertEquals(Status::Success, $response->getStatus());
        $this->assertNotNull($response->getData());
        $this->assertSame(['col1', 'col2'], ProtobufHelper::repeatedStringToArray($response->getData()->getColumns()));
        $this->assertCount(0, $response->getData()->getRows());
        $this->assertStringContainsString('successfully', $response->getMessage());
        $this->assertSame('[]', json_encode($this->getRows($response)));
    }

    /**
     * @dataProvider commandProvider
     * @param callable(self):ExecuteQueryCommand $command
     */
    public function testExecuteAlterTable(callable $command): void
    {
        $this->createTable(
            $this->projectCredentials,
            $this->workspaceName,
            'test_table',
            [
                'columns' => [
                    'col1' => [
                        'type' => Bigquery::TYPE_INTEGER,
                        'length' => '',
                        'nullable' => false,
                    ],
                    'col2' => [
                        'type' => Bigquery::TYPE_STRING,
                        'length' => '10',
                        'nullable' => true,
                    ],
                ],
            ],
        );
        $query = sprintf(
            <<<SQL
                ALTER TABLE %s ADD COLUMN `col3` STRING
            SQL,
            BigqueryQuote::quoteSingleIdentifier('test_table'),
        );
        $command = $command($this)->setQuery($query);

        $handler = (new ExecuteQueryHandler($this->clientManager));
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ExecuteQueryResponse::class, $response);
        $this->assertEquals(Status::Success, $response->getStatus());
        $this->assertNull($response->getData());
        $this->assertStringContainsString('successfully', $response->getMessage());
    }

    /**
     * @return array<mixed>
     */
    private function getRows(ExecuteQueryResponse $response): array
    {
        if ($response->getData() === null) {
            return [];
        }
        /** @var ExecuteQueryResponse\Data\Row[] $rows */
        $rows = iterator_to_array($response->getData()->getRows());
        if (count($rows) === 0) {
            return [];
        }
        return array_map(
            fn(ExecuteQueryResponse\Data\Row $r): array => iterator_to_array($r->getFields()),
            $rows,
        );
    }
}
