<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import;

use DateTime;
use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
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
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        $bucketResponse = $this->createTestBucket($projectCredentials);
        $this->bucketResponse = $bucketResponse;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    private function createDestinationTable(
        string $bucketDatabaseName,
        string $destinationTableName,
        BigQueryClient $bqClient
    ): BigqueryTableDefinition {
        $tableDestDef = new BigqueryTableDefinition(
            $bucketDatabaseName,
            $destinationTableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createGenericColumn('col3'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
            ]),
            []
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
        foreach ([['1', '2', '4', '2014-11-10 13:12:06.000000+00:00'], ['2', '3', '3', '2014-11-10 13:12:06.000000+00:00'], ['3', '3', '3', '2014-11-10 13:12:06.000000+00:00']] as $i) {
            $quotedValues = [];
            foreach ($i as $item) {
                $quotedValues[] = BigqueryQuote::quote($item);
            }
            $sql = sprintf(
                'INSERT %s.%s (`col1`, `col2`, `col3`, `_timestamp`) VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($destinationTableName),
                implode(',', $quotedValues)
            );
            $bqClient->runQuery($bqClient->query($sql));
        }
        return $tableDestDef;
    }

    private function createAccountsTable(
        BigQueryClient $bqClient,
        string $bucketDatabaseName,
        string $destinationTableName
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
            BigqueryQuote::quoteSingleIdentifier($destinationTableName)
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
        string $tableName
    ): void {
        /** @var array<string, array<string>> $timestamps */
        $timestamps = $bqClient->runQuery($bqClient->query(sprintf(
            'SELECT _timestamp FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($database),
            BigqueryQuote::quoteSingleIdentifier($tableName)
        )))->getIterator()->current();
        $timestamps = $timestamps['_timestamp'];
        foreach ($timestamps as $timestamp) {
            $this->assertNotEmpty($timestamp);
            $this->assertEqualsWithDelta(
                new DateTime('now'),
                new DateTime($timestamp),
                60 // set to 1 minute, it's important that timestamp is there
            );
        }
    }
}
