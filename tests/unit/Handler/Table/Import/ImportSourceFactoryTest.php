<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Handler\Table\Import;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\BigQuery\Table as BQTable;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Db\ImportExport\Storage\Bigquery\SelectSource;
use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ColumnsMismatchException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportSourceFactory;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\SourceContext;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\BigQuery\QueryBuilder\TableImportQueryBuilder;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use PHPUnit\Framework\TestCase;

class ImportSourceFactoryTest extends TestCase
{
    /**
     * @param array<array{name: string, type: string, mode: string}> $columns
     */
    private function createMockBigQueryClient(array $columns): BigQueryClient
    {
        $bqClient = $this->createMock(BigQueryClient::class);

        // Mock the dataset and table structure for reflection
        $table = $this->createMock(BQTable::class);
        $table->method('exists')->willReturn(true); // Table exists
        $table->method('info')->willReturn([
            'schema' => [
                'fields' => $columns,
            ],
            'type' => 'TABLE',
        ]);

        $dataset = $this->createMock(Dataset::class);
        $dataset->method('table')->willReturn($table);

        $bqClient->method('dataset')->willReturn($dataset);

        return $bqClient;
    }

    /**
     * @param array<string, string> $columnMappings
     * @param array<string, string|array<string>> $whereFilters
     */
    private function createMockCommand(
        array $columnMappings = [],
        array $whereFilters = [],
        int $limit = 0,
        int $seconds = 0,
    ): TableImportFromTableCommand {
        $command = $this->createMock(TableImportFromTableCommand::class);

        $sourceMapping = $this->createMock(TableImportFromTableCommand\SourceTableMapping::class);

        // Create proper RepeatedField for path
        $path = new RepeatedField(GPBType::STRING);
        $path[] = 'test_dataset';

        $sourceMapping->method('getPath')->willReturn($path);
        $sourceMapping->method('getTableName')->willReturn('source_table');
        $sourceMapping->method('getLimit')->willReturn($limit);
        $sourceMapping->method('getSeconds')->willReturn($seconds);

        // Mock column mappings with real RepeatedField
        $mappingsRepeated = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        if (!empty($columnMappings)) {
            foreach ($columnMappings as $source => $dest) {
                $mapping = $this->createMock(TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class);
                $mapping->method('getSourceColumnName')->willReturn($source);
                $mapping->method('getDestinationColumnName')->willReturn($dest);
                $mappingsRepeated[] = $mapping;
            }
        }
        $sourceMapping->method('getColumnMappings')->willReturn($mappingsRepeated);

        // Mock WHERE filters with real RepeatedField
        $filtersRepeated = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        if (!empty($whereFilters)) {
            foreach ($whereFilters as $columnName => $values) {
                $filter = $this->createMock(TableWhereFilter::class);
                $filter->method('getColumnsName')->willReturn($columnName);

                // Mock the values as RepeatedField
                $filterValues = new RepeatedField(GPBType::STRING);
                foreach ((array) $values as $value) {
                    $filterValues[] = $value;
                }
                $filter->method('getValues')->willReturn($filterValues);
                $filter->method('getOperator')->willReturn(Operator::eq); // Use Operator enum constant
                $filter->method('getDataType')->willReturn(DataType::STRING); // Use DataType constant (integer)

                $filtersRepeated[] = $filter;
            }
        }
        $sourceMapping->method('getWhereFilters')->willReturn($filtersRepeated);

        $command->method('getSource')->willReturn($sourceMapping);

        return $command;
    }

    public function testCreateFromCommandWithAllColumns(): void
    {
        $columns = [
            ['name' => 'id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'],
            ['name' => 'name', 'type' => 'STRING', 'mode' => 'NULLABLE'],
            ['name' => 'created_at', 'type' => 'TIMESTAMP', 'mode' => 'NULLABLE'],
        ];

        $bqClient = $this->createMockBigQueryClient($columns);
        $command = $this->createMockCommand();

        $factory = new ImportSourceFactory($bqClient);
        $result = $factory->createFromCommand($command);

        $this->assertInstanceOf(SourceContext::class, $result);
        $this->assertInstanceOf(Table::class, $result->source);
        $this->assertCount(3, $result->effectiveDefinition->getColumnsDefinitions());
        $this->assertEquals(['id', 'name', 'created_at'], $result->selectedColumns);
    }

    public function testCreateFromCommandWithSelectedColumns(): void
    {
        $columns = [
            ['name' => 'id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'],
            ['name' => 'name', 'type' => 'STRING', 'mode' => 'NULLABLE'],
            ['name' => 'created_at', 'type' => 'TIMESTAMP', 'mode' => 'NULLABLE'],
        ];

        $bqClient = $this->createMockBigQueryClient($columns);
        $command = $this->createMockCommand(['id' => 'id', 'name' => 'name']);

        // Use real TableImportQueryBuilder since it's final
        $queryBuilder = new TableImportQueryBuilder($bqClient, new ColumnConverter());

        $factory = new ImportSourceFactory($bqClient, $queryBuilder);
        $result = $factory->createFromCommand($command);

        $this->assertInstanceOf(SourceContext::class, $result);
        // Should use SelectSource because not all columns are selected
        $this->assertInstanceOf(SelectSource::class, $result->source);
        $this->assertCount(2, $result->effectiveDefinition->getColumnsDefinitions());
        $this->assertEquals(['id', 'name'], $result->selectedColumns);
    }

    public function testCreateFromCommandWithWhereFilter(): void
    {
        $columns = [
            ['name' => 'id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'],
            ['name' => 'name', 'type' => 'STRING', 'mode' => 'NULLABLE'],
            ['name' => 'status', 'type' => 'STRING', 'mode' => 'NULLABLE'],
        ];

        $bqClient = $this->createMockBigQueryClient($columns);
        $command = $this->createMockCommand([], ['status' => ['active']]);

        // Use real TableImportQueryBuilder
        $queryBuilder = new TableImportQueryBuilder($bqClient, new ColumnConverter());

        $factory = new ImportSourceFactory($bqClient, $queryBuilder);
        $result = $factory->createFromCommand($command);

        $this->assertInstanceOf(SourceContext::class, $result);
        // Should use SelectSource because of WHERE filter
        $this->assertInstanceOf(SelectSource::class, $result->source);
    }

    public function testCreateFromCommandWithLimit(): void
    {
        $columns = [
            ['name' => 'id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'],
            ['name' => 'name', 'type' => 'STRING', 'mode' => 'NULLABLE'],
        ];

        $bqClient = $this->createMockBigQueryClient($columns);
        $command = $this->createMockCommand([], [], 100);

        // Use real TableImportQueryBuilder
        $queryBuilder = new TableImportQueryBuilder($bqClient, new ColumnConverter());

        $factory = new ImportSourceFactory($bqClient, $queryBuilder);
        $result = $factory->createFromCommand($command);

        $this->assertInstanceOf(SourceContext::class, $result);
        // Should use SelectSource because of LIMIT
        $this->assertInstanceOf(SelectSource::class, $result->source);
    }

    public function testCreateFromCommandWithTimeTravel(): void
    {
        $columns = [
            ['name' => 'id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'],
            ['name' => 'name', 'type' => 'STRING', 'mode' => 'NULLABLE'],
        ];

        $bqClient = $this->createMockBigQueryClient($columns);
        $command = $this->createMockCommand([], [], 0, 3600); // 1 hour ago

        // Use real TableImportQueryBuilder
        $queryBuilder = new TableImportQueryBuilder($bqClient, new ColumnConverter());

        $factory = new ImportSourceFactory($bqClient, $queryBuilder);
        $result = $factory->createFromCommand($command);

        $this->assertInstanceOf(SourceContext::class, $result);
        // Should use SelectSource because of time travel
        $this->assertInstanceOf(SelectSource::class, $result->source);
    }

    public function testCreateFromCommandThrowsOnInvalidColumn(): void
    {
        $columns = [
            ['name' => 'id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'],
            ['name' => 'name', 'type' => 'STRING', 'mode' => 'NULLABLE'],
        ];

        $bqClient = $this->createMockBigQueryClient($columns);
        // Request a column that doesn't exist
        $command = $this->createMockCommand(['id' => 'id', 'nonexistent' => 'nonexistent']);

        $factory = new ImportSourceFactory($bqClient);

        $this->expectException(ColumnsMismatchException::class);
        $this->expectExceptionMessage('Column "nonexistent" not found in source table');

        $factory->createFromCommand($command);
    }

    public function testCreateFromCommandWithColumnRenaming(): void
    {
        $columns = [
            ['name' => 'id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'],
            ['name' => 'old_name', 'type' => 'STRING', 'mode' => 'NULLABLE'],
        ];

        $bqClient = $this->createMockBigQueryClient($columns);
        $command = $this->createMockCommand(['id' => 'id', 'old_name' => 'new_name']);

        // Use real TableImportQueryBuilder
        $queryBuilder = new TableImportQueryBuilder($bqClient, new ColumnConverter());

        $factory = new ImportSourceFactory($bqClient, $queryBuilder);
        $result = $factory->createFromCommand($command);

        $this->assertInstanceOf(SourceContext::class, $result);
        $this->assertEquals(['id', 'old_name'], $result->selectedColumns);
    }

    public function testCreateFromCommandCaseInsensitiveColumnLookup(): void
    {
        $columns = [
            ['name' => 'ID', 'type' => 'INTEGER', 'mode' => 'REQUIRED'],
            ['name' => 'Name', 'type' => 'STRING', 'mode' => 'NULLABLE'],
        ];

        $bqClient = $this->createMockBigQueryClient($columns);
        // Request columns with different case
        $command = $this->createMockCommand(['id' => 'id', 'name' => 'name']);

        // Use real TableImportQueryBuilder
        $queryBuilder = new TableImportQueryBuilder($bqClient, new ColumnConverter());

        $factory = new ImportSourceFactory($bqClient, $queryBuilder);
        $result = $factory->createFromCommand($command);

        $this->assertInstanceOf(SourceContext::class, $result);
        $this->assertEquals(['id', 'name'], $result->selectedColumns);
    }
}
