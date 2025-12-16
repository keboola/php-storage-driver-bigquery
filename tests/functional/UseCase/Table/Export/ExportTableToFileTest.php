<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Storage\StorageObject;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\Backend\BigQuery\Clustering;
use Keboola\StorageDriver\Backend\BigQuery\RangePartitioning;
use Keboola\StorageDriver\BigQuery\Handler\Table\BadExportFilterParametersException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Export\ColumnNotFoundException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Export\ExportTableToFileHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\ImportExportShared;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileResponse;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Throwable;

/**
 * @group Export
 */
class ExportTableToFileTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];

        $this->bucketResponse = $this->createTestBucket($this->projects[0][0]);
    }

    /**
     * @dataProvider simpleExportProvider
     * @param array{exportOptions: ExportOptions} $input
     * @param array<int, string>[]|null $exportData
     */
    public function testExportTableToFile(array $input, ?array $exportData): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_Test_table_export';
        $exportDir = sprintf(
            'export/%s/',
            str_replace([' ', '"', '\''], ['-', '_', '_'], $this->getTestHash()),
        );

        // cleanup
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $this->dropSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // create table
        $sourceTableDef = $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        $this->clearGCSBucketDir(
            (string) getenv('BQ_BUCKET_NAME'),
            $exportDir,
        );

        // export command
        $response = $this->exportTable($bucketDatabaseName, $sourceTableName, $input, $exportDir);

        $exportedTableInfo = $response->getTableInfo();
        $this->assertNotNull($exportedTableInfo);

        $this->assertSame($sourceTableName, $exportedTableInfo->getTableName());
        $this->assertSame([$bucketDatabaseName], ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPath()));
        $this->assertSame(
            $sourceTableDef->getPrimaryKeysNames(),
            ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPrimaryKeysNames()),
        );
        /** @var TableInfo\TableColumn[] $columns */
        $columns = iterator_to_array($exportedTableInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableInfo\TableColumn $col) => $col->getName(),
            $columns,
        );
        $this->assertSame($sourceTableDef->getColumnsNames(), $columnsNames);

        // check files
        $files = $this->listFilesSimple(
            (string) getenv('BQ_BUCKET_NAME'),
            $exportDir,
        );
        $this->assertNotNull($files);
        $this->assertCount(2, $files);

        // check data
        if ($exportData !== null) {
            $csvData = $this->getExportAsCsv((string) getenv('BQ_BUCKET_NAME'), $exportDir);
            $this->assertEqualsArrays(
                $exportData,
                $csvData,
            );
        }
    }

    /**
     * @dataProvider slicedExportProvider
     * @param string[] $expectedFileNames file names without the hash directory prefix
     */
    public function testExportTableToSlicedFile(bool $isCompressed, array $expectedFileNames): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_Test_table_export_sliced';
        $exportDir = sprintf(
            'export/%s/',
            $this->getTestHash(),
        );
        // Build expected file paths dynamically using the test hash
        $expectedFiles = array_map(
            fn(string $fileName) => sprintf('export/%s/%s', $this->getTestHash(), $fileName),
            $expectedFileNames,
        );

        // cleanup
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $this->dropSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // create table from file
        $this->createSourceTableFromFile(
            $bqClient,
            'import',
            'big_table.csv.gz',
            true,
            $bucketDatabaseName,
            $sourceTableName,
            [
                'FID',
                'NAZEV',
                'Y',
                'X',
                'KONTAKT',
                'SUBKATEGORIE',
                'KATEGORIE',
                'Column6',
                'Column7',
                'Column8',
                'Column9',
                'GlobalID',
            ],
        );

        $this->clearGCSBucketDir(
            (string) getenv('BQ_BUCKET_NAME'),
            $exportDir,
        );

        // export command
        $cmd = new TableExportToFileCommand();

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($sourceTableName),
        );

        $cmd->setFileProvider(FileProvider::GCS);

        $cmd->setFileFormat(FileFormat::CSV);

        $exportOptions = new ExportOptions();
        $exportOptions->setIsCompressed($isCompressed);
        $cmd->setExportOptions($exportOptions);

        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath($exportDir)
                ->setFileName('exp'),
        );

        $handler = new ExportTableToFileHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(TableExportToFileResponse::class, $response);

        // check files
        $files = $this->listFilesSimple(
            (string) getenv('BQ_BUCKET_NAME'),
            $exportDir,
        );
        $this->assertSame($expectedFiles, $files['files']);
    }

    public function testExportTableToFileLimitColumns(): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = $this->getTestHash() . '_Test_table_export';
        $exportDir = sprintf(
            'export/%s/',
            str_replace([' ', '"', '\''], ['-', '_', '_'], $this->getTestHash()),
        );

        // cleanup
        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $this->dropSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // create table
        $sourceTableDef = $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // clear files
        $this->clearGCSBucketDir(
            (string) getenv('BQ_BUCKET_NAME'),
            $exportDir,
        );

        // export command
        $cmd = new TableExportToFileCommand();

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($sourceTableName),
        );
        $cmd->setFileProvider(FileProvider::GCS);
        $cmd->setFileFormat(FileFormat::CSV);

        $columnsToExport = new RepeatedField(GPBType::STRING);
        $columnsToExport[] = 'col1';
        $columnsToExport[] = 'col2';
        // we did skip col3

        $exportOptions = new ExportOptions();
        $exportOptions->setIsCompressed(false);
        $exportOptions->setColumnsToExport($columnsToExport);
        $cmd->setExportOptions($exportOptions);

        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath($exportDir),
        );

        $handler = new ExportTableToFileHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(TableExportToFileResponse::class, $response);

        $exportedTableInfo = $response->getTableInfo();
        $this->assertNotNull($exportedTableInfo);

        $this->assertSame($sourceTableName, $exportedTableInfo->getTableName());
        $this->assertSame([$bucketDatabaseName], ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPath()));
        $this->assertSame(
            $sourceTableDef->getPrimaryKeysNames(),
            ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPrimaryKeysNames()),
        );
        /** @var TableInfo\TableColumn[] $columns */
        $columns = iterator_to_array($exportedTableInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableInfo\TableColumn $col) => $col->getName(),
            $columns,
        );
        $this->assertSame($sourceTableDef->getColumnsNames(), $columnsNames);

        $csvData = $this->getExportAsCsv((string) getenv('BQ_BUCKET_NAME'), $exportDir);
        $this->assertEqualsArrays(
            [
                ['1', '2'],
                ['2', '3'],
                ['3', '3'],
            ],
            // data are not trimmed because IE lib doesn't do so. TD serves them in raw form prefixed by space
            $csvData,
        );
    }

    public function simpleExportProvider(): Generator
    {
        yield 'plain csv' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                ]),
            ],
            [ // expected data
                ['1', '2', '4', '2022-01-01 12:00:01 UTC'],
                ['2', '3', '4', '2022-01-01 12:00:02 UTC'],
                ['3', '3', '3', '2022-01-01 12:00:03 UTC'],
            ],
        ];
        yield 'gzipped csv' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => true,
                ]),
            ],
            null, // expected data - it's gzip file, not csv
        ];
        yield 'filter columns' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1', 'col2'],
                ]),
            ],
            [ // expected data
                ['1', '2'],
                ['2', '3'],
                ['3', '3'],
            ],
        ];
        yield 'filter order by' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1'],
                    'orderBy' => [
                        new ImportExportShared\ExportOrderBy([
                            'columnName' => 'col1',
                            'order' => ImportExportShared\ExportOrderBy\Order::DESC,
                        ]),
                    ],
                ]),
            ],
            [ // expected data
                ['3'],
                ['2'],
                ['1'],
            ],
        ];
        yield 'filter order by with dataType' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1'],
                    'orderBy' => [
                        new ImportExportShared\ExportOrderBy([
                            'columnName' => 'col1',
                            'order' => ImportExportShared\ExportOrderBy\Order::DESC,
                            'dataType' => DataType::INTEGER,
                        ]),
                    ],
                ]),
            ],
            [ // expected data
                ['3'],
                ['2'],
                ['1'],
            ],
        ];

        yield 'filter limit' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'limit' => 2,
                    ]),
                    'orderBy' => [
                        new ImportExportShared\ExportOrderBy([
                            'columnName' => 'col1',
                            'order' => ImportExportShared\ExportOrderBy\Order::ASC,
                            'dataType' => DataType::INTEGER,
                        ]),
                    ],
                ]),
            ],
            [ // expected data
                ['1'],
                ['2'],
            ],
        ];
        yield 'filter changedSince + changedUntil' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1', '_timestamp'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'changeSince' => '1641038401',
                        'changeUntil' => '1641038402',
                    ]),
                ]),
            ],
            [ // expected data
                ['1', '2022-01-01 12:00:01 UTC'],
            ],
        ];
        yield 'filter changedSince + changedUntil as whereFilter' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1', '_timestamp'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => '_timestamp',
                                'operator' => Operator::ge,
                                'values' => ['1641038401'],
                                'dataType' => DataType::TIMESTAMP,
                            ]),
                            new TableWhereFilter([
                                'columnsName' => '_timestamp',
                                'operator' => Operator::lt,
                                'values' => ['1641038402'],
                                'dataType' => DataType::TIMESTAMP,
                            ]),
                        ],
                    ]),
                ]),
            ],
            [ // expected data
                ['1', '2022-01-01 12:00:01 UTC'],
            ],
        ];
        yield 'filter simple where' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'col2',
                                'operator' => Operator::ge,
                                'values' => ['3'],
                            ]),
                        ],
                    ]),
                ]),
            ],
            [ // expected data
                ['2'],
                ['3'],
            ],
        ];
        yield 'filter multiple where' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'col2',
                                'operator' => Operator::ge,
                                'values' => ['3'],
                            ]),
                            new TableWhereFilter([
                                'columnsName' => 'col3',
                                'operator' => Operator::lt,
                                'values' => ['4'],
                            ]),
                        ],
                    ]),
                ]),
            ],
            [ // expected data
                ['3'],
            ],
        ];
        yield 'filter where with dataType' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['col1'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'col2',
                                'operator' => Operator::gt,
                                'values' => ['2.9'],
                                'dataType' => DataType::REAL,
                            ]),
                            new TableWhereFilter([
                                'columnsName' => 'col2',
                                'operator' => Operator::lt,
                                'values' => ['3.1'],
                                'dataType' => DataType::REAL,
                            ]),
                        ],
                    ]),
                ]),
            ],
            [ // expected data
                ['2'],
                ['3'],
            ],
        ];
    }

    /**
     * @param array{exportOptions: ExportOptions} $exportOptions
     */
    public function exportTable(
        string $bucketDatabaseName,
        string $sourceTableName,
        array $exportOptions,
        string $exportDir,
    ): TableExportToFileResponse {
        $cmd = new TableExportToFileCommand();

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($sourceTableName),
        );

        $cmd->setFileProvider(FileProvider::GCS);

        $cmd->setFileFormat(FileFormat::CSV);

        if ($exportOptions['exportOptions'] instanceof ExportOptions) {
            $cmd->setExportOptions($exportOptions['exportOptions']);
        }

        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath($exportDir)
                ->setFileName('exp'),
        );

        $response = (new ExportTableToFileHandler($this->clientManager))(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(TableExportToFileResponse::class, $response);
        return $response;
    }

    /**
     * @phpcs:ignore
     * @param array{exportOptions: ExportOptions} $params
     * @dataProvider filterProvider
     */
    public function testTablePreviewWithWrongTypesInWhereFilters(array $params, string $expectExceptionMessage): void
    {
        $tableName = $this->getTestHash() . '_Test_table';
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $tableStructure = [
            'columns' => [
                'int' => [
                    'type' => Bigquery::TYPE_INTEGER,
                    'length' => '',
                    'nullable' => true,
                ],
                'date' => [
                    'type' => Bigquery::TYPE_DATE,
                    'length' => '',
                    'nullable' => true,
                ],
                'datetime' => [
                    'type' => Bigquery::TYPE_DATETIME,
                    'length' => '',
                    'nullable' => true,
                ],
                'time' => [
                    'type' => Bigquery::TYPE_TIME,
                    'length' => '',
                    'nullable' => true,
                ],
                'varchar' => [
                    'type' => Bigquery::TYPE_STRING,
                    'length' => '200',
                    'nullable' => true,
                ],
                'timestamp' => [
                    'type' => Bigquery::TYPE_TIMESTAMP,
                    'length' => '',
                    'nullable' => false,
                ],
            ],
            'primaryKeysNames' => [],
        ];
        $this->createTable($this->projectCredentials, $bucketDatabaseName, $tableName, $tableStructure);

        // FILL DATA
        $insertGroups = [
            [
                'columns' => '`int`, `date`, `datetime`, `time`, `varchar`, `timestamp`',
                'rows' => [
                    "200, '2022-01-01', '2022-01-01 12:00:02', '12:35:00', 'xxx', '1989-08-31 00:00:00.000'",
                ],
            ],
        ];
        $this->fillTableWithData($this->projectCredentials, $bucketDatabaseName, $tableName, $insertGroups);

        $exportDir = sprintf(
            'export/%s/',
            str_replace([' ', '"', '\''], ['-', '_', '_'], $this->getTestHash()),
        );
        try {
            $this->exportTable($bucketDatabaseName, $tableName, $params, $exportDir);
            $this->fail('This should never happen');
        } catch (BadExportFilterParametersException|ColumnNotFoundException $e) {
            $this->assertStringContainsString($expectExceptionMessage, $e->getMessage());
        }
    }

    public function filterProvider(): Generator
    {
        yield 'non exist columns' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['non-exist'],
                ]),
            ],
            'Name non-exist not found inside ',
        ];
    }

    public function slicedExportProvider(): Generator
    {
        // File names only - the full path with hash is built dynamically in the test
        yield 'plain csv' => [
            false, // compression
            [
                'exp000000000000.csv',
                'exp000000000001.csv',
                'expmanifest',
            ],
        ];
        yield 'gzipped csv' => [
            true, // compression
            [
                'exp000000000000.csv.gz',
                'exp000000000001.csv.gz',
                'expmanifest',
            ],
        ];
    }

    private function createSourceTable(
        string $databaseName,
        string $tableName,
        BigQueryClient $bqClient,
    ): BigqueryTableDefinition {
        $tableDef = new BigqueryTableDefinition(
            $databaseName,
            $tableName,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
                BigqueryColumn::createGenericColumn('col3'),
                BigqueryColumn::createTimestampColumn('_timestamp'),
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

        // init some values
        $insert = [];
        foreach ([
                     ['\'1\'', '\'2\'', '\'4\'', '\'2022-01-01 12:00:01\''],
                     ['\'2\'', '\'3\'', '\'4\'', '\'2022-01-01 12:00:02\''],
                     ['\'3\'', '\'3\'', '\'3\'', '\'2022-01-01 12:00:03\''],
                 ] as $i) {
            $insert[] = sprintf('(%s)', implode(',', $i));
        }

        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES %s',
            BigqueryQuote::quoteSingleIdentifier($databaseName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
            implode(',', $insert),
        )));

        return $tableDef;
    }

    private function dropSourceTable(
        string $databaseName,
        string $tableName,
        BigQueryClient $bqClient,
    ): void {
        $bucket = $bqClient->dataset($databaseName);
        $table = $bucket->table($tableName);
        if (!$table->exists()) {
            return;
        }
        $qb = new BigqueryTableQueryBuilder();
        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($databaseName, $tableName),
        ));
    }

    /**
     * @param string[] $sourceColumns
     */
    private function createSourceTableFromFile(
        BigQueryClient $bqClient,
        string $sourceFilePath,
        string $sourceFileName,
        bool $sourceFileIsCompressed,
        string $destinationDatabaseName,
        string $destinationTableName,
        array $sourceColumns,
    ): void {
        // create table
        $columnsLines = [];
        foreach ($sourceColumns as $column) {
            $columnsLines[] = sprintf(
                '%s STRING',
                $column,
            );
        }
        $bqClient->runQuery($bqClient->query(
            sprintf(
                'CREATE TABLE %s.%s (
                    %s
                );',
                BigqueryQuote::quoteSingleIdentifier($destinationDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($destinationTableName),
                implode(",\n", $columnsLines),
            ),
        ));

        // import data to table
        $cmd = new TableImportFromFileCommand();
        $cmd->setFileProvider(FileProvider::GCS);
        $cmd->setFileFormat(FileFormat::CSV);

        $columns = new RepeatedField(GPBType::STRING);
        foreach ($sourceColumns as $column) {
            $columns[] = $column;
        }
        $formatOptions = new Any();
        $formatOptions->pack(
            (new TableImportFromFileCommand\CsvTypeOptions())
                ->setColumnsNames($columns)
                ->setDelimiter(CsvOptions::DEFAULT_DELIMITER)
                ->setEnclosure(CsvOptions::DEFAULT_ENCLOSURE)
                ->setEscapedBy(CsvOptions::DEFAULT_ESCAPED_BY)
                ->setSourceType(TableImportFromFileCommand\CsvTypeOptions\SourceType::SINGLE_FILE)
                ->setCompression($sourceFileIsCompressed
                    ? TableImportFromFileCommand\CsvTypeOptions\Compression::GZIP
                    : TableImportFromFileCommand\CsvTypeOptions\Compression::NONE),
        );
        $cmd->setFormatTypeOptions($formatOptions);

        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath($sourceFilePath)
                ->setFileName($sourceFileName),
        );

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $destinationDatabaseName;
        $cmd->setDestination(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($destinationTableName),
        );

        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportExportShared\ImportOptions())
                ->setImportType(ImportExportShared\ImportOptions\ImportType::FULL)
                ->setDedupType(ImportExportShared\ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1),
        );

        $handler = new ImportTableFromFileHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $handler(
            $this->projectCredentials,
            $cmd,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    /**
     * @param array<int, array<string, mixed>> $expectedFiles
     * @param StorageObject[] $files
     */
    public static function assertFilesMatch(array $expectedFiles, array $files): void
    {
        self::assertCount(count($expectedFiles), $files);
        /** @var array{fileName: string, size: int} $expectedFile */
        foreach ($expectedFiles as $i => $expectedFile) {
            /** @var array{Key: string, Size: string} $actualFile */
            $actualFile = $files[$i];
            self::assertStringContainsString((string) $expectedFile['fileName'], (string) $actualFile['Key']);
            $fileSize = (int) $actualFile['Size'];
            $expectedFileSize = ((int) $expectedFile['size']) * 1024 * 1024;
            // check that the file size is in range xMB +- 1 000 000B
            //  - (because I cannot really say what the exact size in bytes should be)
            if ($expectedFileSize !== 0) {
                self::assertTrue(
                    ($expectedFileSize - 1000000) < $fileSize && $fileSize < ($expectedFileSize + 100000),
                    sprintf('Actual size is %s but expected is %s', $fileSize, $expectedFileSize),
                );
            }
        }
    }

    /**
     * @return array<mixed>
     */
    private function getExportAsCsv(string $bucket, string $prefix): array
    {
        $files = $this->listGCSFiles($bucket, $prefix);
        $result = [];
        foreach ($files as $file) {
            $csvData = array_map('str_getcsv', explode(PHP_EOL, $file->downloadAsString()));
            array_pop($csvData);
            $result = [...$result, ...$csvData];
        }

        return $result;
    }

    public function testPartitionedTableWithRequirePartitionFilter(): void
    {
        $tableName = $this->getTestHash() . '_Test_table';
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setNullable(false)
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = (new TableColumnShared)
            ->setName('time')
            ->setType(Bigquery::TYPE_TIMESTAMP)
            ->setNullable(false);
        $any = new Any();
        $any->pack(
            (new CreateTableCommand\BigQueryTableMeta())
                ->setClustering((new Clustering())->setFields(['id']))
                ->setRangePartitioning(
                    (new RangePartitioning())
                        ->setField('id')
                        ->setRange(
                            (new RangePartitioning\Range())
                                ->setStart('0')
                                ->setEnd('10')
                                ->setInterval('1'),
                        ),
                )
                ->setRequirePartitionFilter(true),
        );
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns)
            ->setMeta($any);
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $exportDir = sprintf(
            'export/%s/',
            str_replace([' ', '"', '\''], ['-', '_', '_'], $this->getTestHash()),
        );
        try {
            $this->exportTable(
                $bucketDatasetName,
                $tableName,
                [
                    'exportOptions' => (new ExportOptions([
                        'isCompressed' => false,
                        'columnsToExport' => ['id'],
                    ])),
                ],
                $exportDir,
            );
            $this->fail('This should never happen');
        } catch (Throwable $e) {
            // 'Cannot query over table 'xxx.xxx' without a filter over column(s) 'id'
            // that can be used for partition elimination'
            $this->assertInstanceOf(BadExportFilterParametersException::class, $e);
            $this->assertStringContainsString(
                'without a filter over column(s) \'id\' that can be used for partition elimination',
                $e->getMessage(),
            );
        }
    }
}
