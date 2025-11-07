<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import;

use DateTime;
use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Timestamp;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;

class BaseImportTestCase extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];

        $bucketResponse = $this->createTestBucket($this->projects[0][0]);
        $this->bucketResponse = $bucketResponse;
    }

    protected function createDestinationTable(
        string $bucketDatabaseName,
        string $destinationTableName,
        BigQueryClient $bqClient,
    ): BigqueryTableDefinition {
        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                new BigqueryColumn(
                    'col1',
                    new Bigquery(Bigquery::TYPE_STRING, [
                        'length' => '32000',
                        'nullable' => false,
                    ]),
                ),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createGenericColumn('col3'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $query = $bqClient->query($sql);
        $bqClient->runQuery($query);
        // init some values
        // phpcs:ignore
        foreach ([
                     ['1', '2', '4', '2014-11-10 13:12:06.000000+00:00'],
                     ['2', '3', '3', '2014-11-10 13:12:06.000000+00:00'],
                     ['3', '3', '3', '2014-11-10 13:12:06.000000+00:00'],
                 ] as $i) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = BigqueryQuote::quote($item);
            }
            $sql = sprintf(
                'INSERT %s.%s (`col1`, `col2`, `col3`, `_timestamp`) VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($destinationTableName),
                implode(',', $quotedValues),
            );
            $bqClient->runQuery($bqClient->query($sql));
        }
        return $tableDestDef;
    }

    protected function createDestinationTypedTable(
        string $bucketDatabaseName,
        string $destinationTableName,
        BigQueryClient $bqClient,
    ): BigqueryTableDefinition {
        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                new BigqueryColumn('col1', new Bigquery(
                    Bigquery::TYPE_INT,
                    [],
                )),
                new BigqueryColumn('col2', new Bigquery(
                    Bigquery::TYPE_BIGINT,
                    [],
                )),
                new BigqueryColumn('col3', new Bigquery(
                    Bigquery::TYPE_INT,
                    [],
                )),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            [],
        );
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getCreateTableCommand(
            $tableDestDef->getSchemaName(),
            $tableDestDef->getTableName(),
            $tableDestDef->getColumnsDefinitions(),
            $tableDestDef->getPrimaryKeysNames(),
        );
        $bqClient->runQuery($bqClient->query($sql));
        // init some values
        foreach ([
                     ['1', '2', '4', BigqueryQuote::quote('2014-11-10 13:12:06.000000+00:00')],
                     ['2', '3', '3', BigqueryQuote::quote('2014-11-10 13:12:06.000000+00:00')],
                     ['3', '3', '3', BigqueryQuote::quote('2014-11-10 13:12:06.000000+00:00')],
                 ] as $i) {
            $queryJobConfiguration = $bqClient->query(sprintf(
                'INSERT %s.%s (`col1`, `col2`, `col3`, `_timestamp`) VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($destinationTableName),
                implode(',', $i),
            ));
            $bqClient->runQuery($queryJobConfiguration);
        }
        return $tableDestDef;
    }

    protected function createAccountsTable(
        BigQueryClient $bqClient,
        string $bucketDatabaseName,
        string $destinationTableName,
    ): void {
        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE TABLE %s.%s (
                `id` STRING(60),
                `idTwitter` STRING(60),
                `name` STRING(100),
                `import` STRING(60),
                `isImported` STRING(60),
                `apiLimitExceededDatetime` STRING(60),
                `analyzeSentiment` STRING(60),
                `importKloutScore` STRING(60),
                `timestamp` STRING(60),
                `oauthToken` STRING(60),
                `oauthSecret` STRING(60),
                `idApp` STRING(60),
                `_timestamp` TIMESTAMP
            );',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
    }

    /**
     * @return Generator<string,array{int}>
     */
    public function importCompressionProvider(): Generator
    {
        yield 'NO Compression' => [
            TableImportFromFileCommand\CsvTypeOptions\Compression::NONE,
        ];
        yield 'GZIP' => [
            TableImportFromFileCommand\CsvTypeOptions\Compression::GZIP,
        ];
    }

    protected function assertTimestamp(
        BigQueryClient $bqClient,
        string $database,
        string $tableName,
    ): void {
        $queryResults = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT _timestamp FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($database),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        )));

        $hasTimestamp = false;
        foreach ($queryResults as $row) {
            $hasTimestamp = true;
            $timestamp = $row['_timestamp'] ?? null;
            $this->assertNotEmpty($timestamp);
            $this->assertEqualsWithDelta(
                new DateTime('now'),
                new DateTime((string) $timestamp),
                60, // set to 1 minute, it's important that timestamp is there
            );
        }

        $this->assertTrue($hasTimestamp, 'Expected table to contain _timestamp values.');
    }

    /**
     * @param string[] $columns
     * @return array<int, array<string, mixed>>
     */
    protected function fetchTable(
        BigQueryClient $client,
        string $schemaName,
        string $tableName,
        array $columns = [],
    ): array {
        if (count($columns) === 0) {
            $result = $client->runQuery($client->query(sprintf(
                'SELECT * FROM %s.%s',
                $schemaName,
                $tableName,
            )));
        } else {
            $result = $client->runQuery($client->query(sprintf(
                'SELECT %s FROM %s.%s',
                implode(', ', array_map(static function ($item) {
                    return BigqueryQuote::quoteSingleIdentifier($item);
                }, $columns)),
                BigqueryQuote::quoteSingleIdentifier($schemaName),
                BigqueryQuote::quoteSingleIdentifier($tableName),
            )));
        }

        $result = iterator_to_array($result);
        /** @var array<int, array<string, mixed>> $result */
        foreach ($result as &$row) {
            foreach ($row as &$item) {
                if ($item instanceof Timestamp) {
                    $item = $item->get()->format(DateTimeHelper::FORMAT);
                }
            }
        }

        return $result;
    }

    /**
     * @return Generator<string,array{boolean}>
     */
    public function typedTablesProvider(): Generator
    {
        yield 'typed ' => [true,];
        yield 'string table ' => [false,];
    }
}
