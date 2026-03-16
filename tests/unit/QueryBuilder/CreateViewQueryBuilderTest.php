<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\QueryBuilder;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\QueryBuilder\CreateViewQueryBuilder;
use Keboola\StorageDriver\BigQuery\QueryBuilder\QueryBuilderException;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use PHPUnit\Framework\TestCase;

class CreateViewQueryBuilderTest extends TestCase
{
    private CreateViewQueryBuilder $qb;

    protected function setUp(): void
    {
        $this->qb = new CreateViewQueryBuilder();
    }

    public function testSelectAllColumns(): void
    {
        $sql = $this->qb->buildCreateViewSql(
            'my_dataset',
            'my_view',
            'source_dataset',
            'source_table',
            [],
            new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class),
        );

        self::assertSame(
            'CREATE OR REPLACE VIEW `my_dataset`.`my_view` AS (SELECT * FROM `source_dataset`.`source_table`)',
            $sql,
        );
    }

    public function testSelectSpecificColumns(): void
    {
        $sql = $this->qb->buildCreateViewSql(
            'ds',
            'v',
            'src_ds',
            'src_tbl',
            ['col_a', 'col_b', 'col_c'],
            new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class),
        );

        self::assertSame(
            'CREATE OR REPLACE VIEW `ds`.`v` AS (SELECT `col_a`, `col_b`, `col_c` FROM `src_ds`.`src_tbl`)',
            $sql,
        );
    }

    /**
     * @dataProvider provideSingleValueOperators
     */
    public function testWhereSingleValue(int $operator, string $expectedSqlOp): void
    {
        $filter = new TableWhereFilter();
        $filter->setColumnsName('status');
        $filter->setOperator($operator);
        $values = new RepeatedField(GPBType::STRING);
        $values[] = 'active';
        $filter->setValues($values);

        $filters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $filters[] = $filter;

        $sql = $this->qb->buildCreateViewSql('ds', 'v', 'src', 'tbl', [], $filters);

        self::assertSame(
            sprintf(
                "CREATE OR REPLACE VIEW `ds`.`v` AS (SELECT * FROM `src`.`tbl` WHERE `status` %s 'active')",
                $expectedSqlOp,
            ),
            $sql,
        );
    }

    /**
     * @return iterable<string, array{int, string}>
     */
    public static function provideSingleValueOperators(): iterable
    {
        yield 'eq' => [Operator::eq, '='];
        yield 'ne' => [Operator::ne, '<>'];
        yield 'gt' => [Operator::gt, '>'];
        yield 'ge' => [Operator::ge, '>='];
        yield 'lt' => [Operator::lt, '<'];
        yield 'le' => [Operator::le, '<='];
    }

    public function testWhereMultipleValuesIn(): void
    {
        $filter = new TableWhereFilter();
        $filter->setColumnsName('color');
        $filter->setOperator(Operator::eq);
        $values = new RepeatedField(GPBType::STRING);
        $values[] = 'red';
        $values[] = 'blue';
        $values[] = 'green';
        $filter->setValues($values);

        $filters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $filters[] = $filter;

        $sql = $this->qb->buildCreateViewSql('ds', 'v', 'src', 'tbl', [], $filters);

        self::assertSame(
            "CREATE OR REPLACE VIEW `ds`.`v` AS (SELECT * FROM `src`.`tbl` WHERE `color` IN ('red', 'blue', 'green'))",
            $sql,
        );
    }

    public function testWhereMultipleValuesNotIn(): void
    {
        $filter = new TableWhereFilter();
        $filter->setColumnsName('color');
        $filter->setOperator(Operator::ne);
        $values = new RepeatedField(GPBType::STRING);
        $values[] = 'red';
        $values[] = 'blue';
        $filter->setValues($values);

        $filters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $filters[] = $filter;

        $sql = $this->qb->buildCreateViewSql('ds', 'v', 'src', 'tbl', [], $filters);

        self::assertSame(
            "CREATE OR REPLACE VIEW `ds`.`v` AS (SELECT * FROM `src`.`tbl` WHERE `color` NOT IN ('red', 'blue'))",
            $sql,
        );
    }

    public function testWhereMultipleFiltersAnd(): void
    {
        $filter1 = new TableWhereFilter();
        $filter1->setColumnsName('status');
        $filter1->setOperator(Operator::eq);
        $v1 = new RepeatedField(GPBType::STRING);
        $v1[] = 'active';
        $filter1->setValues($v1);

        $filter2 = new TableWhereFilter();
        $filter2->setColumnsName('age');
        $filter2->setOperator(Operator::gt);
        $v2 = new RepeatedField(GPBType::STRING);
        $v2[] = '18';
        $filter2->setValues($v2);

        $filters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $filters[] = $filter1;
        $filters[] = $filter2;

        $sql = $this->qb->buildCreateViewSql('ds', 'v', 'src', 'tbl', [], $filters);

        self::assertSame(
            "CREATE OR REPLACE VIEW `ds`.`v` AS (SELECT * FROM `src`.`tbl` WHERE `status` = 'active' AND `age` > '18')",
            $sql,
        );
    }

    public function testEmptyValuesThrowsException(): void
    {
        $filter = new TableWhereFilter();
        $filter->setColumnsName('status');
        $filter->setOperator(Operator::eq);
        $filter->setValues(new RepeatedField(GPBType::STRING));

        $filters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $filters[] = $filter;

        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage('WHERE filter for column "status" must have at least one value.');

        $this->qb->buildCreateViewSql('ds', 'v', 'src', 'tbl', [], $filters);
    }

    public function testEmptyColumnNameThrowsException(): void
    {
        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage('Column name must not be empty.');

        $this->qb->buildCreateViewSql(
            'ds',
            'v',
            'src',
            'tbl',
            ['valid_col', ''],
            new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class),
        );
    }

    public function testUnsupportedOperatorWithMultipleValuesThrowsException(): void
    {
        $filter = new TableWhereFilter();
        $filter->setColumnsName('age');
        $filter->setOperator(Operator::gt);
        $values = new RepeatedField(GPBType::STRING);
        $values[] = '10';
        $values[] = '20';
        $filter->setValues($values);

        $filters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $filters[] = $filter;

        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessageMatches('/does not support multiple values/');

        $this->qb->buildCreateViewSql('ds', 'v', 'src', 'tbl', [], $filters);
    }

    public function testColumnsWithWhereFilters(): void
    {
        $filter = new TableWhereFilter();
        $filter->setColumnsName('status');
        $filter->setOperator(Operator::eq);
        $v = new RepeatedField(GPBType::STRING);
        $v[] = 'active';
        $filter->setValues($v);

        $filters = new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class);
        $filters[] = $filter;

        $sql = $this->qb->buildCreateViewSql('ds', 'v', 'src', 'tbl', ['id', 'name'], $filters);

        self::assertSame(
            "CREATE OR REPLACE VIEW `ds`.`v` AS (SELECT `id`, `name` FROM `src`.`tbl` WHERE `status` = 'active')",
            $sql,
        );
    }

    public function testCrossDatasetView(): void
    {
        $sql = $this->qb->buildCreateViewSql(
            'linked_dataset',
            'alias_view',
            'source_dataset',
            'source_table',
            ['col1'],
            new RepeatedField(GPBType::MESSAGE, TableWhereFilter::class),
        );

        self::assertSame(
            'CREATE OR REPLACE VIEW `linked_dataset`.`alias_view`'
            . ' AS (SELECT `col1` FROM `source_dataset`.`source_table`)',
            $sql,
        );
    }
}
