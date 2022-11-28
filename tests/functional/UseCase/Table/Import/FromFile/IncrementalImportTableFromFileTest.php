<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\FromFile;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\CsvOptions\CsvOptions;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use LogicException;
use Throwable;

class IncrementalImportTableFromFileTest extends BaseImportTestCase
{
    public function testImportTableFromTableIncrementalLoad(): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);

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
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->clientManager);
        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                []
            );
            $this->fail('Should fail incremental import is not implemented');
            //$ref = new BigqueryTableReflection($db, $bucketDatabaseName, $destinationTableName);
            // 2 not unique rows from destination + 1 unique row from source
            // + 1 row which is dedup of two duplicates in source and one from destination
            //$this->assertSame(4, $ref->getRowsCount());
            // @todo test updated values
        } catch (LogicException $e) {
            $this->assertSame('Not implemented', $e->getMessage());
        }

        // cleanup
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $bqClient->runQuery($bqClient->query($sql));
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

    public function testImportTableFromTableIncrementalSlicedWithDeduplication(): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);

        $this->createAccountsTable($bqClient, $bucketDatabaseName, $destinationTableName);
        // init some values
        $bqClient->runQuery($bqClient->query(sprintf(
        // phpcs:ignore
            'INSERT INTO %s.%s VALUES (\'10\',\'448810375\',\'init\',\'0\',\'1\',\'\',\'1\',\'0\',\'2012-02-20 09:34:22\',\'ddd\',\'ddd\',\'1\',\'2012-02-20 09:34:22\')',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
        // this line should be updated
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
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::NONE)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath('sliced/accounts')
                ->setFileName('S3.accounts.csvmanifest')
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->clientManager);
        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                []
            );
            $this->fail('Should fail incremental import not implemented.');
            //$ref = new BigqueryTableReflection($db, $bucketDatabaseName, $destinationTableName);
            // 1 row from destination + 1 row from destination updated + 1 row from slices new
            //$this->assertSame(3, $ref->getRowsCount());
        } catch (Throwable $e) {
            $this->assertSame('Not implemented', $e->getMessage());
        }

        // cleanup
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName);
        $bqClient->runQuery($bqClient->query($sql));
    }

    public function testImportTableFromTableIncrementalSlicedCompressedWithDeduplication(): void
    {
        $destinationTableName = md5($this->getName()) . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);

        $this->createAccountsTable($bqClient, $bucketDatabaseName, $destinationTableName);
        // init some values
        $bqClient->runQuery($bqClient->query(sprintf(
        // phpcs:ignore
            'INSERT INTO %s.%s VALUES (\'10\',\'448810375\',\'init\',\'0\',\'1\',\'\',\'1\',\'0\',\'2012-02-20 09:34:22\',\'ddd\',\'ddd\',\'1\',\'2012-02-20 09:34:22\')',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier($destinationTableName),
        )));
        // this line should be updated
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
                ->setCompression(TableImportFromFileCommand\CsvTypeOptions\Compression::GZIP)
        );
        $cmd->setFormatTypeOptions($formatOptions);
        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath('sliced/accounts-gzip')
                ->setFileName('S3.accounts-gzip.csvmanifest')
        );
        $cmd->setDestination(
            (new Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );
        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportOptions())
                ->setImportType(ImportOptions\ImportType::INCREMENTAL)
                ->setDedupType(ImportOptions\DedupType::UPDATE_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(0)
                ->setTimestampColumn('_timestamp')
        );

        $handler = new ImportTableFromFileHandler($this->clientManager);
        try {
            $handler(
                $this->projectCredentials,
                $cmd,
                []
            );
            $this->fail('Should fail incremental import not implemented.');
            //$ref = new BigqueryTableReflection($db, $bucketDatabaseName, $destinationTableName);
            // 1 row from destination + 1 row from destination updated + 1 row from slices new
            //$this->assertSame(3, $ref->getRowsCount());
        } catch (Throwable $e) {
            $this->assertSame('Not implemented', $e->getMessage());
        }

        // cleanup
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getDropTableCommand($bucketDatabaseName, $destinationTableName);
        $bqClient->runQuery($bqClient->query($sql));
    }
}
