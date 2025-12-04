<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\QueryBuilder;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery as BigqueryDefinition;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\BigQuery\QueryBuilder\TableImportQueryBuilder;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use PHPUnit\Framework\TestCase;

class TableImportQueryBuilderTest extends TestCase
{
    public function testFilterColumnOutsideMappingDoesNotChangeSelectList(): void
    {
        $builder = new TableImportQueryBuilder(
            $this->createMock(BigQueryClient::class),
            new ColumnConverter(),
        );

        $columnDefinitions = new ColumnCollection([
            new BigqueryColumn('Id', new BigqueryDefinition(BigqueryDefinition::TYPE_INT)),
            new BigqueryColumn('Name', new BigqueryDefinition(BigqueryDefinition::TYPE_STRING)),
            new BigqueryColumn('Iso', new BigqueryDefinition(BigqueryDefinition::TYPE_STRING)),
        ]);
        $tableDefinition = new BigqueryTableDefinition('dataset', 'source_table', false, $columnDefinitions, []);

        $sourceMapping = new TableImportFromTableCommand\SourceTableMapping();
        $sourceMapping->setPath(['dataset']);
        $sourceMapping->setTableName('source_table');

        $columnMappings = new RepeatedField(
            GPBType::MESSAGE,
            TableImportFromTableCommand\SourceTableMapping\ColumnMapping::class,
        );
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('Id')
            ->setDestinationColumnName('dest_id');
        $columnMappings[] = (new TableImportFromTableCommand\SourceTableMapping\ColumnMapping())
            ->setSourceColumnName('Name')
            ->setDestinationColumnName('dest_name');
        $sourceMapping->setColumnMappings($columnMappings);

        $whereFilters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $whereFilters[] = new TableWhereFilter([
            'columnsName' => 'iso',
            'operator' => Operator::eq,
            'values' => ['US'],
            'dataType' => DataType::STRING,
        ]);
        $sourceMapping->setWhereFilters($whereFilters);

        $response = $builder->buildSelectSourceSql(
            $tableDefinition,
            ['Id', 'Name'],
            $sourceMapping,
        );

        self::assertSame(
            'SELECT `source_table`.`Id`, `source_table`.`Name` FROM `dataset`.`source_table` '
            . 'WHERE `source_table`.`iso` = @dcValue1',
            $response->getQuery(),
        );
        self::assertSame(['dcValue1' => 'US'], $response->getBindings());
    }
}
