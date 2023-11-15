<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\FromFile;

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
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportStrategy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\FunctionalTests\UseCase\Table\Import\BaseImportTestCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

/**
 * @group Import
 */
class IncrementalImportTableFromFileTest extends BaseImportTestCase
{
    /**
     * @dataProvider typedTablesProvider
     */
    public function testImportTableFromFileIncrementalLoad(bool $isTypedTable): void
    {
        $destinationTableName = $this->getTestHash() . '_Test_table_final';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // create tables
        if ($isTypedTable) {
            $tableDestDef = $this->createDestinationTypedTable($bucketDatabaseName, $destinationTableName, $bqClient);
        } else {
            $tableDestDef = $this->createDestinationTable($bucketDatabaseName, $destinationTableName, $bqClient);
        }
        $ref = new BigqueryTableReflection($bqClient, $bucketDatabaseName, $destinationTableName);
        $this->assertSame(3, $ref->getRowsCount());

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
                ->setImportStrategy($isTypedTable ? ImportStrategy::USER_DEFINED_TABLE : ImportStrategy::STRING_TABLE)
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
        // 2 not unique rows from destination + 1 unique row from source
        // + 1 row which is dedup of two duplicates in source and one from destination
        $this->assertSame(4, $ref->getRowsCount());
        $this->assertTimestamp($bqClient, $bucketDatabaseName, $destinationTableName);
        $data = $this->fetchTable(
            $bqClient,
            $bucketDatabaseName,
            $destinationTableName,
            ['col1', 'col2', 'col3']
        );
        $this->assertEqualsCanonicalizing([
            [
                'col1' => '3',
                'col2' => '3',
                'col3' => '3',
            ],
            [
                'col1' => '2',
                'col2' => '3',
                'col3' => '3',
            ],
            [
                'col1' => '1',
                'col2' => '2',
                'col3' => '3',
            ],
            [
                'col1' => '5',
                'col2' => '2',
                'col3' => '3',
            ],
        ], $data);

        // cleanup
        $qb = new BigqueryTableQueryBuilder();
        $sql = $qb->getDropTableCommand($tableDestDef->getSchemaName(), $tableDestDef->getTableName());
        $bqClient->runQuery($bqClient->query($sql));
    }
}
