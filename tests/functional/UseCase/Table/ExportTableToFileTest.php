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
use Keboola\StorageDriver\BigQuery\Handler\Table\BadExportFilterParametersException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Export\ExportTableToFileHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\ImportExportShared;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileFormat;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FilePath;
use Keboola\StorageDriver\Command\Table\ImportExportShared\FileProvider;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
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

class ExportTableToFileTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateBucketResponse $bucketResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials,] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        $this->bucketResponse = $this->createTestBucket($projectCredentials);
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    /**
     * @dataProvider simpleExportProvider
     * @param array{exportOptions: ExportOptions} $input
     * @param array<int, string>[]|null $exportData
     */
    public function testExportTableToFile(array $input, ?array $exportData): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = md5($this->getName()) . '_Test_table_export';
        $exportDir = sprintf(
            'export/%s/',
            str_replace([' ', '"', '\''], ['-', '_', '_'], $this->getName())
        );

        // create table
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $sourceTableDef = $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        $this->clearGCSBucketDir(
            (string) getenv('BQ_BUCKET_NAME'),
            $exportDir
        );

        // export command
        $response = $this->exportTable($bucketDatabaseName, $sourceTableName, $input, $exportDir);

        $exportedTableInfo = $response->getTableInfo();
        $this->assertNotNull($exportedTableInfo);

        $this->assertSame($sourceTableName, $exportedTableInfo->getTableName());
        $this->assertSame([$bucketDatabaseName], ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPath()));
        $this->assertSame(
            $sourceTableDef->getPrimaryKeysNames(),
            ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPrimaryKeysNames())
        );
        /** @var TableInfo\TableColumn[] $columns */
        $columns = iterator_to_array($exportedTableInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableInfo\TableColumn $col) => $col->getName(),
            $columns
        );
        $this->assertSame($sourceTableDef->getColumnsNames(), $columnsNames);

        // check files
        $files = $this->listFilesSimple(
            (string) getenv('BQ_BUCKET_NAME'),
            $exportDir
        );
        $this->assertNotNull($files);
        $this->assertCount(2, $files);

        // check data
        if ($exportData !== null) {
            $csvData = $this->getExportAsCsv((string) getenv('BQ_BUCKET_NAME'), $exportDir);
            $this->assertEqualsArrays(
                $exportData,
                $csvData
            );
        }

        // cleanup
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $this->dropSourceTable($sourceTableDef->getSchemaName(), $sourceTableDef->getTableName(), $bqClient);
    }

    /**
     * @dataProvider slicedExportProvider
     * @param string[] $expectedFiles
     */
    public function testExportTableToSlicedFile(bool $isCompressed, array $expectedFiles): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = md5($this->getName()) . '_Test_table_export_sliced';
        $exportDir = sprintf(
            'export/%s/',
            md5($this->getName())
        );

        // cleanup
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
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
            ]
        );

        $this->clearGCSBucketDir(
            (string) getenv('BQ_BUCKET_NAME'),
            $exportDir
        );

        // export command
        $cmd = new TableExportToFileCommand();

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($sourceTableName)
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
                ->setFileName('exp')
        );

        $handler = new ExportTableToFileHandler($this->clientManager);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            []
        );

        $this->assertInstanceOf(TableExportToFileResponse::class, $response);

        // check files
        $files = $this->listFilesSimple(
            (string) getenv('BQ_BUCKET_NAME'),
            $exportDir
        );
        $this->assertSame($expectedFiles, $files['files']);

        // cleanup
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $this->dropSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);
    }

    public function testExportTableToFileLimitColumns(): void
    {
        $bucketDatabaseName = $this->bucketResponse->getCreateBucketObjectName();
        $sourceTableName = md5($this->getName()) . '_Test_table_export';
        $exportDir = sprintf(
            'export/%s/',
            str_replace([' ', '"', '\''], ['-', '_', '_'], $this->getName())
        );

        // create table
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $sourceTableDef = $this->createSourceTable($bucketDatabaseName, $sourceTableName, $bqClient);

        // clear files
        $this->clearGCSBucketDir(
            (string) getenv('BQ_BUCKET_NAME'),
            $exportDir
        );

        // export command
        $cmd = new TableExportToFileCommand();

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($sourceTableName)
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
                ->setPath($exportDir)
        );

        $handler = new ExportTableToFileHandler($this->clientManager);
        $response = $handler(
            $this->projectCredentials,
            $cmd,
            []
        );

        $this->assertInstanceOf(TableExportToFileResponse::class, $response);

        $exportedTableInfo = $response->getTableInfo();
        $this->assertNotNull($exportedTableInfo);

        $this->assertSame($sourceTableName, $exportedTableInfo->getTableName());
        $this->assertSame([$bucketDatabaseName], ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPath()));
        $this->assertSame(
            $sourceTableDef->getPrimaryKeysNames(),
            ProtobufHelper::repeatedStringToArray($exportedTableInfo->getPrimaryKeysNames())
        );
        /** @var TableInfo\TableColumn[] $columns */
        $columns = iterator_to_array($exportedTableInfo->getColumns()->getIterator());
        $columnsNames = array_map(
            static fn(TableInfo\TableColumn $col) => $col->getName(),
            $columns
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
            $csvData
        );

        // cleanup
        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);
        $this->dropSourceTable($sourceTableDef->getSchemaName(), $sourceTableDef->getTableName(), $bqClient);
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
                ['1','2022-01-01 12:00:01 UTC'],
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
        string $exportDir
    ): TableExportToFileResponse {
        $cmd = new TableExportToFileCommand();

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $cmd->setSource(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($sourceTableName)
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
                ->setFileName('exp')
        );

        $response = (new ExportTableToFileHandler($this->clientManager))(
            $this->projectCredentials,
            $cmd,
            []
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
        $tableName = md5($this->getName()) . '_Test_table';
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
            str_replace([' ', '"', '\''], ['-', '_', '_'], $this->getName())
        );
        try {
            $this->exportTable($bucketDatabaseName, $tableName, $params, $exportDir);
            $this->fail('This should never happen');
        } catch (BadExportFilterParametersException $e) {
            $this->assertStringContainsString($expectExceptionMessage, $e->getMessage());
        }
    }

    public function filterProvider(): Generator
    {
        yield 'wrong int' => [
            [ // input
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['int'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'int',
                                'operator' => Operator::eq,
                                'values' => ['aaa'],
                            ]),
                        ],
                    ]),
                ]),
            ],
            'Invalid filter value, expected:"INT64", actual:"STRING".',
        ];

        yield 'wrong date' => [
            [
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['date'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'date',
                                'operator' => Operator::eq,
                                'values' => ['2022-02-31'],
                            ]),
                        ],
                    ]),
                ]),
            ],
            // non-existing date
            'Invalid date: \'2022-02-31\'; while executing the filter on column \'date\'; Column \'date\'',
        ];

        yield 'wrong time' => [
            [
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['time'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'time',
                                'operator' => Operator::eq,
                                'values' => ['25:59:59.999999'],
                            ]),
                        ],
                    ]),
                ]),
            ],
            'Invalid time string "25:59:59.999999"; while executing the filter on column \'time\'; Column \'time\'',
        ];

        yield 'wrong timestamp' => [
            [
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['timestamp'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'timestamp',
                                'operator' => Operator::eq,
                                'values' => ['25:59:59.999999'],
                            ]),
                        ],
                    ]),
                ]),
            ],
            //phpcs:ignore
            "Invalid timestamp: '25:59:59.999999'; while executing the filter on column 'timestamp'; Column 'timestamp'",
        ];

        yield 'wrong more filters' => [
            [
                'exportOptions' => new ExportOptions([
                    'isCompressed' => false,
                    'columnsToExport' => ['int'],
                    'filters' => new ImportExportShared\ExportFilters([
                        'whereFilters' => [
                            new TableWhereFilter([
                                'columnsName' => 'int',
                                'operator' => Operator::lt,
                                'values' => ['aaa'],
                            ]),
                            new TableWhereFilter([
                                'columnsName' => 'int',
                                'operator' => Operator::gt,
                                'values' => ['aaa'],
                            ]),
                            new TableWhereFilter([
                                'columnsName' => 'time',
                                'operator' => Operator::eq,
                                'values' => ['25:59:59.999999'],
                            ]),
                        ],
                    ]),
                ]),
            ],
            'Invalid filter value, expected:"INT64", actual:"STRING".',
        ];
    }


    public function slicedExportProvider(): Generator
    {
        yield 'plain csv' => [
            false, // compression
            [
                'export/1d855d619357989e2544891957ffd565/exp000000000000.csv',
                'export/1d855d619357989e2544891957ffd565/exp000000000001.csv',
                'export/1d855d619357989e2544891957ffd565/expmanifest',
            ],
        ];
        yield 'gzipped csv' => [
            true, // compression
            [
                'export/fedd6d661573483354212e9303f96743/exp000000000000.csv.gz',
                'export/fedd6d661573483354212e9303f96743/exp000000000001.csv.gz',
                'export/fedd6d661573483354212e9303f96743/expmanifest',
            ],
        ];
    }

    private function createSourceTable(
        string $databaseName,
        string $tableName,
        BigQueryClient $bqClient
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
            []
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
        foreach ([
                     ['\'1\'', '\'2\'', '\'4\'', '\'2022-01-01 12:00:01\''],
                     ['\'2\'', '\'3\'', '\'4\'', '\'2022-01-01 12:00:02\''],
                     ['\'3\'', '\'3\'', '\'3\'', '\'2022-01-01 12:00:03\''],
                 ] as $i) {
            $bqClient->runQuery($bqClient->query(sprintf(
                'INSERT INTO %s.%s VALUES (%s)',
                BigqueryQuote::quoteSingleIdentifier($databaseName),
                BigqueryQuote::quoteSingleIdentifier($tableName),
                implode(',', $i)
            )));
        }

        return $tableDef;
    }

    private function dropSourceTable(
        string $databaseName,
        string $tableName,
        BigQueryClient $bqClient
    ): void {
        $bucket = $bqClient->dataset($databaseName);
        $table = $bucket->table($tableName);
        if (!$table->exists()) {
            return;
        }
        $qb = new BigqueryTableQueryBuilder();
        $bqClient->runQuery($bqClient->query(
            $qb->getDropTableCommand($databaseName, $tableName)
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
        array $sourceColumns
    ): void {
        // create table
        $columnsLines = [];
        foreach ($sourceColumns as $column) {
            $columnsLines[] = sprintf(
                '%s STRING',
                $column
            );
        }
        $bqClient->runQuery($bqClient->query(
            sprintf(
                'CREATE TABLE %s.%s (
                    %s
                );',
                BigqueryQuote::quoteSingleIdentifier($destinationDatabaseName),
                BigqueryQuote::quoteSingleIdentifier($destinationTableName),
                implode(",\n", $columnsLines)
            )
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
                    : TableImportFromFileCommand\CsvTypeOptions\Compression::NONE)
        );
        $cmd->setFormatTypeOptions($formatOptions);

        $cmd->setFilePath(
            (new FilePath())
                ->setRoot((string) getenv('BQ_BUCKET_NAME'))
                ->setPath($sourceFilePath)
                ->setFileName($sourceFileName)
        );

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $destinationDatabaseName;
        $cmd->setDestination(
            (new ImportExportShared\Table())
                ->setPath($path)
                ->setTableName($destinationTableName)
        );

        $dedupCols = new RepeatedField(GPBType::STRING);
        $cmd->setImportOptions(
            (new ImportExportShared\ImportOptions())
                ->setImportType(ImportExportShared\ImportOptions\ImportType::FULL)
                ->setDedupType(ImportExportShared\ImportOptions\DedupType::INSERT_DUPLICATES)
                ->setDedupColumnsNames($dedupCols)
                ->setConvertEmptyValuesToNullOnColumns(new RepeatedField(GPBType::STRING))
                ->setNumberOfIgnoredLines(1)
        );

        $handler = new ImportTableFromFileHandler($this->clientManager);
        $handler(
            $this->projectCredentials,
            $cmd,
            []
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
                    sprintf('Actual size is %s but expected is %s', $fileSize, $expectedFileSize)
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
}
