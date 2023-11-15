<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\FromFile;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\CsvOptions\CsvOptions;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\GCSCredentials;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Table\TableImportResponse;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\Import\ImportValidationException;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

/**
 * @group Import
 */
class ImportTableFromFileTest extends BaseImportTestCase
{
    public function testImportTableFromFileFullLoadWithDeduplication(): void
    {
        $destinationTableName = $this->getTestHash() . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // create tables
        $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $bqClient);

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileProvider(FileProvider::GCS);
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'col1';
        $columns[] = 'col2';
        $columns[] = 'col3';
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SINGLE_FILE)
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::NONE)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath('import')
                ->setFileName('a_b_c-3row.csv')
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $dedupCols[] = 'col1';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        // nothing from destination and 3 rows from source dedup to two
        $this->assertSame(2, $ref->getRowsCount());
        $this->assertTimestamp($bqClient, $bucketDatabaseName, $destinationTableName);
        $data = $this->fetchTable(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            ['col1', 'col2', 'col3']
        );
        $this->assertEqualsCanonicalizing([
            [
                'col1' => '5',
                'col2' => '2',
                'col3' => '3',
            ],
            [
                'col1' => '1',
                'col2' => '2',
                'col3' => '3',
            ],
        ], $data);

        // cleanup
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $bqClient->runQuery($bqClient->query($sql));
    }

    public function testImportTableFromFileFullLoadWithoutDeduplication(): void
    {
        $destinationTableName = $this->getTestHash() . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // create tables
        $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $bqClient);

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileProvider(FileProvider::GCS);
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'col1';
        $columns[] = 'col2';
        $columns[] = 'col3';
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SINGLE_FILE)
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::NONE)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath('import')
                ->setFileName('a_b_c-3row.csv')
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertSame(3, $response->getImportedRowsCount());
        $this->assertSame(['col1', 'col2', 'col3'], iterator_to_array($response->getImportedColumns()));
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        // nothing from destination and 3 rows from source
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // cleanup
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $bqClient->runQuery($bqClient->query($sql));
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

    /**
     * @dataProvider importCompressionProvider
     */
    public function testImportTableFromFileFullLoadSlicedWithoutDeduplication(int $compression): void
    {
        $destinationTableName = $this->getTestHash() . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        $this->createAccountsTable($bqClient, $bucketDatabaseName, $destinationTableName);
        // init some values
        $bqClient->runQuery($bqClient->query(sprintf(
        // phpcs:ignore
            'INSERT INTO %s.%s VALUES (\'10\',\'448810375\',\'init\',\'0\',\'1\',\'\',\'1\',\'0\',\'2012-02-20 09:34:22\',\'ddd\',\'ddd\',\'1\',\'2012-02-20 09:34:22\')',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileProvider(FileProvider::GCS);
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'import';
        $columns[] = 'isImported';
        $columns[] = 'id';
        $columns[] = 'idTwitter';
        $columns[] = 'name';
        $columns[] = 'apiLimitExceededDatetime';
        $columns[] = 'analyzeSentiment';
        $columns[] = 'importKloutScore';
        $columns[] = 'timestamp';
        $columns[] = 'oauthToken';
        $columns[] = 'oauthSecret';
        $columns[] = 'idApp';
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SLICED_FILE)
                ->setCompression($compression)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        if ($compression === TableImportFromFileCommand\CsvTypeOptions\Compression::GZIP) {
            $cmd->setFilePath(
                (new FilePath())
                    ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                    ->setPath('sliced/accounts-gzip')
                    ->setFileName('GCS.accounts-gzip.csvmanifest')
            );
        } else {
            // no compression
            $cmd->setFilePath(
                (new FilePath())
                    ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                    ->setPath('sliced/accounts')
                    ->setFileName('GCS.accounts.csvmanifest')
            );
        }
        $credentials = new Any();
        // here must be credentials because content of manifest are downloaded
        $credentials->pack(
            (new GCSCredentials())
                ->setKey((string) getenv('BQ_PRINCIPAL'))
                ->setSecret((string) getenv('BQ_SECRET'))
        );
        $cmd->setFileCredentials($credentials);
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        /** @var TableImportResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertSame(3, $response->getImportedRowsCount());
        $this->assertSame(
            [
                'import',
                'isImported',
                'id',
                'idTwitter',
                'name',
                'apiLimitExceededDatetime',
                'analyzeSentiment',
                'importKloutScore',
                'timestamp',
                'oauthToken',
                'oauthSecret',
                'idApp',
            ],
            iterator_to_array($response->getImportedColumns())
        );
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        // 0 from destination and 3 rows from source
        $this->assertSame(3, $ref->getRowsCount());
        $this->assertSame($ref->getRowsCount(), $response->getTableRowsCount());

        // cleanup
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName);
        $bqClient->runQuery($bqClient->query($sql));
    }

    public function testImportTableFromFileWithNullValues(): void
    {
        $destinationTableName = $this->getTestHash() . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        $this->createAccountsTableWithNotNull($bqClient, $bucketDatabaseName, $destinationTableName);

        $cmd = new TableImportFromFileCommand();
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setFileProvider(FileProvider::GCS);
        $cmd->setFileFormat(FileFormat::CSV);
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'import';
        $columns[] = 'isImported';
        $columns[] = 'id';
        $columns[] = 'idTwitter';
        $columns[] = 'name';
        $columns[] = 'apiLimitExceededDatetime';
        $columns[] = 'analyzeSentiment';
        $columns[] = 'importKloutScore';
        $columns[] = 'timestamp';
        $columns[] = 'oauthToken';
        $columns[] = 'oauthSecret';
        $columns[] = 'idApp';
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SLICED_FILE)
        );
        $cmd->setFormatTypeOptions($formatOptions);

        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath('sliced/accounts')
                ->setFileName('GCS.accounts.csvmanifest')
        );

        $credentials = new Any();
        // here must be credentials because content of manifest are downloaded
        $credentials->pack(
            (new GCSCredentials())
                ->setKey((string) getenv('BQ_PRINCIPAL'))
                ->setSecret((string) getenv('BQ_SECRET'))
        );
        $cmd->setFileCredentials($credentials);
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $convertCols = new RepeatedField(GPBType::STRING);
        $convertCols[] = 'apiLimitExceededDatetime';
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::FULL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns($convertCols)
                ->setImportStrategy(ImportOptions\ImportStrategy::USER_DEFINED_TABLE)
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        // loading from file will fail on import staging->final table anyway, because staging table has nullable=true
        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                [],
                new RuntimeOptions(['runId' => $this->testRunId]),
            );
            $this->fail('should fail because of nulls in required field');
        } catch (ImportValidationException $e) {
            $this->assertSame('Required field apiLimitExceededDatetime cannot be null', $e->getMessage());
        }

        // cleanup
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName);
        $bqClient->runQuery($bqClient->query($sql));
    }

    protected function createAccountsTableWithNotNull(
        BigQueryClient $bqClient,
        string $bucketDatabaseName,
        string $destinationTableName
    ): void {
        $bqClient->runQuery($bqClient->query(sprintf(
            'CREATE TABLE %s.%s (
                `id` STRING(60),
                `idTwitter` STRING(60),
                `name` STRING(100) NOT NULL,
                `import` STRING(60) NOT NULL,
                `isImported` STRING(60) NOT NULL,
                `apiLimitExceededDatetime` STRING(60) NOT NULL,
                `analyzeSentiment` STRING(60) NOT NULL,
                `importKloutScore` STRING(60) NOT NULL,
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
}
