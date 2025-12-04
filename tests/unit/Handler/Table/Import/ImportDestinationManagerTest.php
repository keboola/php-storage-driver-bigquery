<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Handler\Table\Import;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\BigQuery\QueryJobConfiguration;
use Google\Cloud\BigQuery\QueryResults;
use Google\Cloud\BigQuery\Table as BQTable;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery as BigqueryDatatype;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ColumnsMismatchException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportDestinationManager;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ImportOptions\ImportType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\Table as CommandDestination;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use PHPUnit\Framework\TestCase;

class ImportDestinationManagerTest extends TestCase
{
    /**
     * @param array<array{name: string, type: string, mode: string}> $columns
     */
    private function createMockBigQueryClient(bool $tableExists = true, array $columns = []): BigQueryClient
    {
        $bqClient = $this->createMock(BigQueryClient::class);

        $table = $this->createMock(BQTable::class);

        if ($tableExists) {
            $table->method('exists')->willReturn(true);
            $table->method('info')->willReturn([
                'schema' => [
                    'fields' => $columns,
                ],
                'type' => 'TABLE',
            ]);
        } else {
            $table->method('exists')->willReturn(false);
        }

        $dataset = $this->createMock(Dataset::class);
        $dataset->method('table')->willReturn($table);

        $bqClient->method('dataset')->willReturn($dataset);

        // Mock query() to return a query configuration
        $query = $this->createMock(QueryJobConfiguration::class);
        $bqClient->method('query')->willReturn($query);

        // Mock runQuery for table creation
        $queryResults = $this->createMock(QueryResults::class);
        $bqClient->method('runQuery')->willReturn($queryResults);

        return $bqClient;
    }

    private function createMockDestination(string $tableName = 'dest_table'): CommandDestination
    {
        $destination = $this->createMock(CommandDestination::class);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = 'test_dataset';

        $destination->method('getPath')->willReturn($path);
        $destination->method('getTableName')->willReturn($tableName);

        return $destination;
    }

    /**
     * @param array<string> $dedupColumns
     */
    private function createMockImportOptions(
        int $importType = ImportType::FULL,
        array $dedupColumns = [],
    ): ImportOptions {
        $options = $this->createMock(ImportOptions::class);
        $options->method('getImportType')->willReturn($importType);

        $dedupRepeated = new RepeatedField(GPBType::STRING);
        foreach ($dedupColumns as $col) {
            $dedupRepeated[] = $col;
        }

        $options->method('getDedupColumnsNames')->willReturn($dedupRepeated);

        return $options;
    }

    /**
     * @param array<string, string> $columns
     */
    private function createColumnCollection(array $columns): ColumnCollection
    {
        $columnObjects = [];
        foreach ($columns as $name => $type) {
            $columnObjects[] = new BigqueryColumn(
                $name,
                new BigqueryDatatype($type, ['nullable' => true]),
            );
        }
        return new ColumnCollection($columnObjects);
    }

    public function testResolveDestinationExisting(): void
    {
        $columns = [
            ['name' => 'id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'],
            ['name' => 'name', 'type' => 'STRING', 'mode' => 'NULLABLE'],
        ];

        $bqClient = $this->createMockBigQueryClient(true, $columns);
        $destination = $this->createMockDestination();
        $importOptions = $this->createMockImportOptions();
        $expectedColumns = $this->createColumnCollection(['id' => 'INTEGER', 'name' => 'STRING']);

        $manager = new ImportDestinationManager($bqClient);
        $result = $manager->resolveDestination($destination, $importOptions, $expectedColumns);

        $this->assertInstanceOf(BigqueryTableDefinition::class, $result);
        $this->assertEquals('test_dataset', $result->getSchemaName());
        $this->assertEquals('dest_table', $result->getTableName());
    }

    public function testResolveDestinationCreatesNew(): void
    {
        $bqClient = $this->createMockBigQueryClient(false);
        $destination = $this->createMockDestination('new_table');
        // BigQuery doesn't support primary keys, pass empty dedup columns
        $importOptions = $this->createMockImportOptions(ImportType::FULL, []);
        $expectedColumns = $this->createColumnCollection(['id' => 'INTEGER', 'name' => 'STRING']);

        $manager = new ImportDestinationManager($bqClient);
        $result = $manager->resolveDestination($destination, $importOptions, $expectedColumns);

        $this->assertInstanceOf(BigqueryTableDefinition::class, $result);
        $this->assertEquals('test_dataset', $result->getSchemaName());
        $this->assertEquals('new_table', $result->getTableName());
        $this->assertEquals([], $result->getPrimaryKeysNames());
    }

    public function testResolveDestinationForView(): void
    {
        $bqClient = $this->createMockBigQueryClient(false);
        $destination = $this->createMockDestination('new_view');
        $importOptions = $this->createMockImportOptions(ImportType::VIEW);
        $expectedColumns = $this->createColumnCollection(['id' => 'INTEGER', 'name' => 'STRING']);

        $manager = new ImportDestinationManager($bqClient);
        $result = $manager->resolveDestination($destination, $importOptions, $expectedColumns);

        // Should not create table for VIEW import type
        $this->assertInstanceOf(BigqueryTableDefinition::class, $result);
    }

    public function testResolveDestinationForClone(): void
    {
        $bqClient = $this->createMockBigQueryClient(false);
        $destination = $this->createMockDestination('cloned_table');
        $importOptions = $this->createMockImportOptions(ImportType::PBCLONE);
        $expectedColumns = $this->createColumnCollection(['id' => 'INTEGER', 'name' => 'STRING']);

        $manager = new ImportDestinationManager($bqClient);
        $result = $manager->resolveDestination($destination, $importOptions, $expectedColumns);

        // Should not create table for PBCLONE import type
        $this->assertInstanceOf(BigqueryTableDefinition::class, $result);
    }

    public function testValidateIncrementalSuccess(): void
    {
        $sourceColumns = $this->createColumnCollection([
            'id' => 'INTEGER',
            'name' => 'STRING',
            'email' => 'STRING',
        ]);

        $destColumns = $this->createColumnCollection([
            'id' => 'INTEGER',
            'name' => 'STRING',
            'email' => 'STRING',
        ]);

        $destDefinition = new BigqueryTableDefinition(
            'test_dataset',
            'dest_table',
            false,
            $destColumns,
            ['id'],
        );

        $sourceDefinition = new BigqueryTableDefinition(
            'source_dataset',
            'source_table',
            false,
            $sourceColumns,
            [],
        );

        $bqClient = $this->createMock(BigQueryClient::class);
        $manager = new ImportDestinationManager($bqClient);

        // Should not throw exception
        $manager->validateIncrementalDestination($destDefinition, $sourceColumns, $sourceDefinition);

        $this->assertTrue(true); // Assertion to make PHPUnit happy
    }

    public function testValidateIncrementalFailsMissingColumnInSource(): void
    {
        $sourceColumns = $this->createColumnCollection([
            'id' => 'INTEGER',
            'name' => 'STRING',
        ]);

        $destColumns = $this->createColumnCollection([
            'id' => 'INTEGER',
            'name' => 'STRING',
            'email' => 'STRING', // Extra column in destination
        ]);

        $destDefinition = new BigqueryTableDefinition(
            'test_dataset',
            'dest_table',
            false,
            $destColumns,
            ['id'],
        );

        $sourceDefinition = new BigqueryTableDefinition(
            'source_dataset',
            'source_table',
            false,
            $sourceColumns,
            [],
        );

        $bqClient = $this->createMock(BigQueryClient::class);
        $manager = new ImportDestinationManager($bqClient);

        $this->expectException(ColumnsMismatchException::class);
        $this->expectExceptionMessage('Some columns are missing in source table');
        $this->expectExceptionMessage('email');

        $manager->validateIncrementalDestination($destDefinition, $sourceColumns, $sourceDefinition);
    }

    public function testValidateIncrementalFailsMissingColumnInDestination(): void
    {
        $sourceColumns = $this->createColumnCollection([
            'id' => 'INTEGER',
            'name' => 'STRING',
            'email' => 'STRING', // Extra column in source
        ]);

        $destColumns = $this->createColumnCollection([
            'id' => 'INTEGER',
            'name' => 'STRING',
        ]);

        $destDefinition = new BigqueryTableDefinition(
            'test_dataset',
            'dest_table',
            false,
            $destColumns,
            ['id'],
        );

        $sourceDefinition = new BigqueryTableDefinition(
            'source_dataset',
            'source_table',
            false,
            $sourceColumns,
            [],
        );

        $bqClient = $this->createMock(BigQueryClient::class);
        $manager = new ImportDestinationManager($bqClient);

        $this->expectException(ColumnsMismatchException::class);
        $this->expectExceptionMessage('Some columns are missing in workspace table');
        $this->expectExceptionMessage('email');

        $manager->validateIncrementalDestination($destDefinition, $sourceColumns, $sourceDefinition);
    }

    public function testValidateIncrementalFailsTypeMismatch(): void
    {
        $sourceColumns = $this->createColumnCollection([
            'id' => 'INTEGER',
            'name' => 'INTEGER', // Different type than destination
        ]);

        // Destination has NUMERIC type for 'name' which should mismatch with source INTEGER
        $destColumns = new ColumnCollection([
            new BigqueryColumn('id', new BigqueryDatatype('INTEGER', ['nullable' => true])),
            new BigqueryColumn('name', new BigqueryDatatype('NUMERIC', ['nullable' => true])),
        ]);

        $destDefinition = new BigqueryTableDefinition(
            'test_dataset',
            'dest_table',
            false,
            $destColumns,
            [],
        );

        $sourceDefinition = new BigqueryTableDefinition(
            'source_dataset',
            'source_table',
            false,
            $sourceColumns,
            [],
        );

        $bqClient = $this->createMock(BigQueryClient::class);
        $manager = new ImportDestinationManager($bqClient);

        $this->expectException(ColumnsMismatchException::class);
        $this->expectExceptionMessage('Column definitions mismatch');

        $manager->validateIncrementalDestination($destDefinition, $sourceColumns, $sourceDefinition);
    }

    public function testValidateIncrementalIgnoresSystemColumns(): void
    {
        $sourceColumns = $this->createColumnCollection([
            'id' => 'INTEGER',
            'name' => 'STRING',
        ]);

        // Destination has system column _timestamp
        $destColumns = new ColumnCollection([
            new BigqueryColumn('id', new BigqueryDatatype('INTEGER', ['nullable' => true])),
            new BigqueryColumn('name', new BigqueryDatatype('STRING', ['nullable' => true])),
            new BigqueryColumn('_timestamp', new BigqueryDatatype('TIMESTAMP', ['nullable' => true])),
        ]);

        $destDefinition = new BigqueryTableDefinition(
            'test_dataset',
            'dest_table',
            false,
            $destColumns,
            [],
        );

        $sourceDefinition = new BigqueryTableDefinition(
            'source_dataset',
            'source_table',
            false,
            $sourceColumns,
            [],
        );

        $bqClient = $this->createMock(BigQueryClient::class);
        $manager = new ImportDestinationManager($bqClient);

        // Should not throw exception even though _timestamp is not in source
        $manager->validateIncrementalDestination($destDefinition, $sourceColumns, $sourceDefinition);

        $this->assertTrue(true); // Assertion to make PHPUnit happy
    }

    public function testValidateIncrementalAllowsStringTableConversion(): void
    {
        // Source has typed columns
        $sourceColumns = new ColumnCollection([
            new BigqueryColumn('id', new BigqueryDatatype('INTEGER', ['nullable' => false])),
            new BigqueryColumn('amount', new BigqueryDatatype('NUMERIC', ['nullable' => true])),
        ]);

        // Destination is a string table (all STRING types)
        $destColumns = new ColumnCollection([
            new BigqueryColumn('id', new BigqueryDatatype('STRING', ['nullable' => true])),
            new BigqueryColumn('amount', new BigqueryDatatype('STRING', ['nullable' => true])),
        ]);

        $destDefinition = new BigqueryTableDefinition(
            'test_dataset',
            'dest_table',
            false,
            $destColumns,
            [],
        );

        $sourceDefinition = new BigqueryTableDefinition(
            'source_dataset',
            'source_table',
            false,
            $sourceColumns,
            [],
        );

        $bqClient = $this->createMock(BigQueryClient::class);
        $manager = new ImportDestinationManager($bqClient);

        // Should not throw exception - allows any type -> STRING conversion
        $manager->validateIncrementalDestination($destDefinition, $sourceColumns, $sourceDefinition);

        $this->assertTrue(true);
    }

    public function testCreateTable(): void
    {
        $bqClient = $this->createMock(BigQueryClient::class);
        $queryResults = $this->createMock(QueryResults::class);

        // Create a mock query object to be passed to runQuery
        $query = $this->createMock(QueryJobConfiguration::class);

        // Mock the query() method to return the query config
        $bqClient->method('query')->willReturn($query);

        // Expect runQuery to be called once with the query
        $bqClient->expects($this->once())
            ->method('runQuery')
            ->with($query)
            ->willReturn($queryResults);

        $columns = $this->createColumnCollection([
            'id' => 'INTEGER',
            'name' => 'STRING',
        ]);

        $manager = new ImportDestinationManager($bqClient);
        // BigQuery doesn't support primary keys, pass empty array
        $manager->createTable('test_dataset', 'new_table', $columns, []);

        // Test passes if runQuery was called
    }

    public function testValidateIncrementalCaseInsensitive(): void
    {
        // Source columns with mixed case
        $sourceColumns = new ColumnCollection([
            new BigqueryColumn('ID', new BigqueryDatatype('INTEGER', ['nullable' => true])),
            new BigqueryColumn('Name', new BigqueryDatatype('STRING', ['nullable' => true])),
        ]);

        // Destination columns with different case
        $destColumns = new ColumnCollection([
            new BigqueryColumn('id', new BigqueryDatatype('INTEGER', ['nullable' => true])),
            new BigqueryColumn('name', new BigqueryDatatype('STRING', ['nullable' => true])),
        ]);

        $destDefinition = new BigqueryTableDefinition(
            'test_dataset',
            'dest_table',
            false,
            $destColumns,
            [],
        );

        $sourceDefinition = new BigqueryTableDefinition(
            'source_dataset',
            'source_table',
            false,
            $sourceColumns,
            [],
        );

        $bqClient = $this->createMock(BigQueryClient::class);
        $manager = new ImportDestinationManager($bqClient);

        // Should not throw exception - case-insensitive comparison
        $manager->validateIncrementalDestination($destDefinition, $sourceColumns, $sourceDefinition);

        $this->assertTrue(true);
    }
}
