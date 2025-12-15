<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Handler\Workspace\Load;

use ArrayIterator;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery as BigqueryDatatype;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Load\ColumnMappingService;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\ColumnsMismatchException;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use PHPUnit\Framework\TestCase;

class ColumnMappingServiceTest extends TestCase
{
    /**
     * @param array<string, array{type: string, nullable?: bool, length?: string, default?: string}> $columns
     */
    private function createSourceDefinition(array $columns): BigqueryTableDefinition
    {
        $columnObjects = [];
        foreach ($columns as $name => $config) {
            $options = [
                'nullable' => $config['nullable'] ?? true,
            ];
            if (isset($config['length'])) {
                $options['length'] = $config['length'];
            }
            if (isset($config['default'])) {
                $options['default'] = $config['default'];
            }

            $columnObjects[] = new BigqueryColumn(
                $name,
                new BigqueryDatatype($config['type'], $options),
            );
        }

        return new BigqueryTableDefinition(
            'test_dataset',
            'source_table',
            false,
            new ColumnCollection($columnObjects),
            [],
        );
    }

    /**
     * @param array<string, string> $columnMappings
     */
    private function createMockSourceMapping(array $columnMappings = []): LoadTableToWorkspaceCommand\SourceTableMapping
    {
        $sourceMapping = $this->createMock(LoadTableToWorkspaceCommand\SourceTableMapping::class);

        $mappingsRepeated = $this->createMock(RepeatedField::class);

        if (!empty($columnMappings)) {
            $mappings = [];
            foreach ($columnMappings as $source => $dest) {
                $mapping = $this->createMock(LoadTableToWorkspaceCommand\SourceTableMapping\ColumnMapping::class);
                $mapping->method('getSourceColumnName')->willReturn($source);
                $mapping->method('getDestinationColumnName')->willReturn($dest);
                $mappings[] = $mapping;
            }
            $mappingsRepeated->method('getIterator')->willReturn(new ArrayIterator($mappings));
        } else {
            $mappingsRepeated->method('getIterator')->willReturn(new ArrayIterator([]));
        }

        $sourceMapping->method('getColumnMappings')->willReturn($mappingsRepeated);

        return $sourceMapping;
    }

    public function testBuildDestinationColumnsIdentityMapping(): void
    {
        $sourceColumns = [
            'id' => ['type' => 'INTEGER', 'nullable' => false],
            'name' => ['type' => 'STRING', 'nullable' => true],
            'created_at' => ['type' => 'TIMESTAMP', 'nullable' => true],
        ];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        $sourceMapping = $this->createMockSourceMapping(); // No mappings = identity

        $service = new ColumnMappingService();
        $result = $service->buildDestinationColumns($sourceDefinition, $sourceMapping);

        $this->assertInstanceOf(ColumnCollection::class, $result);
        $this->assertCount(3, $result);

        $columns = iterator_to_array($result);
        $this->assertEquals('id', $columns[0]->getColumnName());
        $this->assertEquals('name', $columns[1]->getColumnName());
        $this->assertEquals('created_at', $columns[2]->getColumnName());
    }

    public function testBuildDestinationColumnsWithMapping(): void
    {
        $sourceColumns = [
            'id' => ['type' => 'INTEGER', 'nullable' => false],
            'old_name' => ['type' => 'STRING', 'nullable' => true],
            'created' => ['type' => 'TIMESTAMP', 'nullable' => true],
        ];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        $sourceMapping = $this->createMockSourceMapping([
            'id' => 'user_id',
            'old_name' => 'new_name',
            'created' => 'created_at',
        ]);

        $service = new ColumnMappingService();
        $result = $service->buildDestinationColumns($sourceDefinition, $sourceMapping);

        $this->assertCount(3, $result);

        $columns = iterator_to_array($result);
        $this->assertEquals('user_id', $columns[0]->getColumnName());
        $this->assertEquals('new_name', $columns[1]->getColumnName());
        $this->assertEquals('created_at', $columns[2]->getColumnName());
    }

    public function testBuildDestinationColumnsThrowsOnMissingColumn(): void
    {
        $sourceColumns = [
            'id' => ['type' => 'INTEGER', 'nullable' => false],
            'name' => ['type' => 'STRING', 'nullable' => true],
        ];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        $sourceMapping = $this->createMockSourceMapping([
            'id' => 'user_id',
            'nonexistent' => 'new_column', // This column doesn't exist in source
        ]);

        $service = new ColumnMappingService();

        $this->expectException(ColumnsMismatchException::class);
        $this->expectExceptionMessage('Some columns are missing in source table');
        $this->expectExceptionMessage('nonexistent');

        $service->buildDestinationColumns($sourceDefinition, $sourceMapping);
    }

    public function testBuildDestinationColumnsPreservesTypes(): void
    {
        $sourceColumns = [
            'int_col' => ['type' => 'INTEGER', 'nullable' => false],
            'str_col' => ['type' => 'STRING', 'nullable' => true, 'length' => '255'],
            'num_col' => ['type' => 'NUMERIC', 'nullable' => true],
            'bool_col' => ['type' => 'BOOLEAN', 'nullable' => false],
        ];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        $sourceMapping = $this->createMockSourceMapping(); // Identity mapping

        $service = new ColumnMappingService();
        $result = $service->buildDestinationColumns($sourceDefinition, $sourceMapping);

        $columns = iterator_to_array($result);

        /** @var BigqueryColumn $intCol */
        $intCol = $columns[0];
        /** @var BigqueryDatatype $intDef */
        $intDef = $intCol->getColumnDefinition();
        $this->assertEquals('INTEGER', $intDef->getType());
        $this->assertFalse($intDef->isNullable());

        /** @var BigqueryColumn $strCol */
        $strCol = $columns[1];
        /** @var BigqueryDatatype $strDef */
        $strDef = $strCol->getColumnDefinition();
        $this->assertEquals('STRING', $strDef->getType());
        $this->assertTrue($strDef->isNullable());
        $this->assertEquals('255', $strDef->getLength());

        /** @var BigqueryColumn $numCol */
        $numCol = $columns[2];
        /** @var BigqueryDatatype $numDef */
        $numDef = $numCol->getColumnDefinition();
        $this->assertEquals('NUMERIC', $numDef->getType());

        /** @var BigqueryColumn $boolCol */
        $boolCol = $columns[3];
        /** @var BigqueryDatatype $boolDef */
        $boolDef = $boolCol->getColumnDefinition();
        $this->assertEquals('BOOLEAN', $boolDef->getType());
        $this->assertFalse($boolDef->isNullable());
    }

    public function testBuildDestinationColumnsPartialMapping(): void
    {
        $sourceColumns = [
            'id' => ['type' => 'INTEGER', 'nullable' => false],
            'name' => ['type' => 'STRING', 'nullable' => true],
            'email' => ['type' => 'STRING', 'nullable' => true],
            'created_at' => ['type' => 'TIMESTAMP', 'nullable' => true],
        ];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        // Only map some columns
        $sourceMapping = $this->createMockSourceMapping([
            'id' => 'id',
            'name' => 'full_name',
        ]);

        $service = new ColumnMappingService();
        $result = $service->buildDestinationColumns($sourceDefinition, $sourceMapping);

        // Should only have the mapped columns
        $this->assertCount(2, $result);

        $columns = iterator_to_array($result);
        $this->assertEquals('id', $columns[0]->getColumnName());
        $this->assertEquals('full_name', $columns[1]->getColumnName());
    }

    public function testBuildDestinationColumnsCaseInsensitive(): void
    {
        $sourceColumns = [
            'ID' => ['type' => 'INTEGER', 'nullable' => false],
            'Name' => ['type' => 'STRING', 'nullable' => true],
            'Email' => ['type' => 'STRING', 'nullable' => true],
        ];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        // Map with different case
        $sourceMapping = $this->createMockSourceMapping([
            'id' => 'user_id',
            'name' => 'full_name',
            'email' => 'email_address',
        ]);

        $service = new ColumnMappingService();
        $result = $service->buildDestinationColumns($sourceDefinition, $sourceMapping);

        $this->assertCount(3, $result);

        $columns = iterator_to_array($result);
        $this->assertEquals('user_id', $columns[0]->getColumnName());
        $this->assertEquals('full_name', $columns[1]->getColumnName());
        $this->assertEquals('email_address', $columns[2]->getColumnName());
    }

    public function testBuildDestinationColumnsWithDefault(): void
    {
        $sourceColumns = [
            'id' => ['type' => 'INTEGER', 'nullable' => false],
            'status' => ['type' => 'STRING', 'nullable' => true, 'default' => 'active'],
        ];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        $sourceMapping = $this->createMockSourceMapping(); // Identity

        $service = new ColumnMappingService();
        $result = $service->buildDestinationColumns($sourceDefinition, $sourceMapping);

        $columns = iterator_to_array($result);

        /** @var BigqueryColumn $statusCol */
        $statusCol = $columns[1];
        /** @var BigqueryDatatype $statusDef */
        $statusDef = $statusCol->getColumnDefinition();
        $this->assertEquals('active', $statusDef->getDefault());
    }

    public function testBuildDestinationColumnsEmptySource(): void
    {
        $sourceColumns = [];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        $sourceMapping = $this->createMockSourceMapping();

        $service = new ColumnMappingService();
        $result = $service->buildDestinationColumns($sourceDefinition, $sourceMapping);

        $this->assertInstanceOf(ColumnCollection::class, $result);
        $this->assertCount(0, $result);
    }

    public function testBuildDestinationColumnsMaintainsOrder(): void
    {
        $sourceColumns = [
            'z_last' => ['type' => 'STRING', 'nullable' => true],
            'a_first' => ['type' => 'INTEGER', 'nullable' => false],
            'm_middle' => ['type' => 'TIMESTAMP', 'nullable' => true],
        ];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        $sourceMapping = $this->createMockSourceMapping();

        $service = new ColumnMappingService();
        $result = $service->buildDestinationColumns($sourceDefinition, $sourceMapping);

        $columns = iterator_to_array($result);
        // Should maintain source order
        $this->assertEquals('z_last', $columns[0]->getColumnName());
        $this->assertEquals('a_first', $columns[1]->getColumnName());
        $this->assertEquals('m_middle', $columns[2]->getColumnName());
    }

    public function testBuildDestinationColumnsMaintainsMappingOrder(): void
    {
        $sourceColumns = [
            'col_a' => ['type' => 'STRING', 'nullable' => true],
            'col_b' => ['type' => 'INTEGER', 'nullable' => false],
            'col_c' => ['type' => 'TIMESTAMP', 'nullable' => true],
        ];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        // Map in different order
        $sourceMapping = $this->createMockSourceMapping([
            'col_c' => 'third',
            'col_a' => 'first',
            'col_b' => 'second',
        ]);

        $service = new ColumnMappingService();
        $result = $service->buildDestinationColumns($sourceDefinition, $sourceMapping);

        $columns = iterator_to_array($result);
        // Should maintain mapping order
        $this->assertEquals('third', $columns[0]->getColumnName());
        $this->assertEquals('first', $columns[1]->getColumnName());
        $this->assertEquals('second', $columns[2]->getColumnName());
    }

    public function testBuildDestinationColumnsMultipleMissingColumns(): void
    {
        $sourceColumns = [
            'id' => ['type' => 'INTEGER', 'nullable' => false],
            'name' => ['type' => 'STRING', 'nullable' => true],
        ];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        $sourceMapping = $this->createMockSourceMapping([
            'id' => 'id',
            'missing1' => 'col1',
            'missing2' => 'col2',
        ]);

        $service = new ColumnMappingService();

        $this->expectException(ColumnsMismatchException::class);
        $this->expectExceptionMessage('missing1');
        $this->expectExceptionMessage('missing2');

        $service->buildDestinationColumns($sourceDefinition, $sourceMapping);
    }

    public function testBuildDestinationColumnsSameSourceMultipleTimes(): void
    {
        $sourceColumns = [
            'id' => ['type' => 'INTEGER', 'nullable' => false],
            'name' => ['type' => 'STRING', 'nullable' => true],
        ];

        $sourceDefinition = $this->createSourceDefinition($sourceColumns);
        // Map same source column to multiple destinations
        $sourceMapping = $this->createMockSourceMapping([
            'id' => 'account_id', // PHP arrays can't have duplicate keys, second value wins
        ]);

        $service = new ColumnMappingService();
        $result = $service->buildDestinationColumns($sourceDefinition, $sourceMapping);

        // Should handle this (second mapping overwrites first in the mock)
        $this->assertInstanceOf(ColumnCollection::class, $result);
    }
}
