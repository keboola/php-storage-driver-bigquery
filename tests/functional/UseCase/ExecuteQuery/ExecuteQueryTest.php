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
