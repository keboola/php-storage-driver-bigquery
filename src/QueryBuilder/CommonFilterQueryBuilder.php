<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use DateTime;
use DateTimeZone;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Query\QueryException;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

abstract class CommonFilterQueryBuilder
{
    public const DEFAULT_CAST_SIZE = 16384;
    public const OPERATOR_SINGLE_VALUE = [
        Operator::eq => '=',
        Operator::ne => '<>',
        Operator::gt => '>',
        Operator::ge => '>=',
        Operator::lt => '<',
        Operator::le => '<=',
    ];
    public const OPERATOR_MULTI_VALUE = [
        Operator::eq => 'IN',
        Operator::ne => 'NOT IN',
    ];

    protected BigQueryClient $bigQueryClient;

    protected ColumnConverter $columnConverter;

    public function __construct(
        BigQueryClient $bigQueryClient,
        ColumnConverter $columnConverter
    ) {
        $this->bigQueryClient = $bigQueryClient;
        $this->columnConverter = $columnConverter;
    }

    private function addSelectLargeString(QueryBuilder $query, string $selectColumnExpresion, string $column): void
    {
//casted value
        $query->addSelect(
            sprintf(
                'SUBSTRING(CAST(%s as STRING), 0, %d) AS %s',
                $selectColumnExpresion,
                self::DEFAULT_CAST_SIZE,
                BigqueryQuote::quoteSingleIdentifier($column)
            )
        );
        //flag if is cast
        $query->addSelect(
            sprintf(
                '(CASE WHEN LENGTH(CAST(%s as STRING)) > %s THEN 1 ELSE 0 END) AS %s',
                BigqueryQuote::quoteSingleIdentifier($column),
                self::DEFAULT_CAST_SIZE,
                BigqueryQuote::quoteSingleIdentifier(uniqid($column))
            )
        );
    }

    private function convertNonStringValue(TableWhereFilter $filter, string $value): string|int|float
    {
        switch (true) {
            case $filter->getDataType() === DataType::INTEGER:
            case $filter->getDataType() === DataType::BIGINT:
                $value = (int) $value;
                break;
            case $filter->getDataType() === DataType::REAL:
            case $filter->getDataType() === DataType::DECIMAL:
            case $filter->getDataType() === DataType::DOUBLE:
                $value = (float) $value;
                break;
        }
        return $value;
    }

    protected function processChangedConditions(string $changeSince, string $changeUntil, QueryBuilder $query): void
    {
        if ($changeSince !== '') {
            $query->andWhere('`_timestamp` >= :changedSince');
            $query->setParameter(
                'changedSince',
                $this->getTimestampFormatted($changeSince),
            );
        }

        if ($changeUntil !== '') {
            $query->andWhere('`_timestamp` < :changedUntil');
            $query->setParameter(
                'changedUntil',
                $this->getTimestampFormatted($changeUntil),
            );
        }
    }

    private function getTimestampFormatted(string $timestamp): string
    {
        return (new DateTime('@' . $timestamp, new DateTimeZone('UTC')))
            ->format('Y-m-d H:i:s');
    }

    /**
     * @param RepeatedField|TableWhereFilter[] $filters
     */
    protected function processWhereFilters(RepeatedField $filters, QueryBuilder $query): void
    {
        foreach ($filters as $whereFilter) {
            $values = ProtobufHelper::repeatedStringToArray($whereFilter->getValues());
            if (count($values) === 1) {
                $this->processSimpleValue($whereFilter, reset($values), $query);
            } else {
                $this->processMultipleValue($whereFilter, $values, $query);
            }
        }
    }

    private function processSimpleValue(TableWhereFilter $filter, string $value, QueryBuilder $query): void
    {
        if ($filter->getDataType() !== DataType::STRING) {
            $columnSql = $this->columnConverter->convertColumnByDataType(
                $filter->getColumnsName(),
                $filter->getDataType()
            );
            $value = $this->convertNonStringValue($filter, $value);
        } else {
            $columnSql = BigqueryQuote::quoteSingleIdentifier($filter->getColumnsName());
        }

        $query->andWhere(
            sprintf(
                '%s %s %s',
                $columnSql,
                self::OPERATOR_SINGLE_VALUE[$filter->getOperator()],
                $query->createNamedParameter($value, $filter->getDataType())
            )
        );
    }

    /**
     * @param string[] $values
     */
    private function processMultipleValue(TableWhereFilter $filter, array $values, QueryBuilder $query): void
    {
        if (!array_key_exists($filter->getOperator(), self::OPERATOR_MULTI_VALUE)) {
            throw new QueryBuilderException(
                'whereFilter with multiple values can be used only with "eq", "ne" operators',
            );
        }

        if ($filter->getDataType() !== DataType::STRING) {
            $columnSql = $this->columnConverter->convertColumnByDataType(
                $filter->getColumnsName(),
                $filter->getDataType()
            );
            $values = array_map(fn(string $value) => $this->convertNonStringValue($filter, $value), $values);
            $param = $query->createNamedParameter($values, Connection::PARAM_INT_ARRAY);
        } else {
            $columnSql = BigqueryQuote::quoteSingleIdentifier($filter->getColumnsName());
            $param = $query->createNamedParameter($values, Connection::PARAM_STR_ARRAY);
        }

        $query->andWhere(
            sprintf(
                '%s %s UNNEST(%s)',
                $columnSql,
                self::OPERATOR_MULTI_VALUE[$filter->getOperator()],
                $param
            )
        );
    }

    /**
     * @param RepeatedField|ExportOrderBy[] $sort
     */
    protected function processOrderStatement(RepeatedField $sort, QueryBuilder $query): void
    {
        try {
            foreach ($sort as $orderBy) {
                if ($orderBy->getDataType() !== DataType::STRING) {
                    $query->addOrderBy(
                        $this->columnConverter->convertColumnByDataType(
                            $orderBy->getColumnName(),
                            $orderBy->getDataType()
                        ),
                        ExportOrderBy\Order::name($orderBy->getOrder())
                    );
                    return;
                }
                $query->addOrderBy(
                    BigqueryQuote::quoteSingleIdentifier($orderBy->getColumnName()),
                    ExportOrderBy\Order::name($orderBy->getOrder())
                );
            }
        } catch (QueryException $e) {
            throw new QueryBuilderException(
                $e->getMessage(),
                $e
            );
        }
    }

    /**
     * @param string[] $columns
     */
    protected function processSelectStatement(
        array $columns,
        QueryBuilder $query,
        ColumnCollection $tableColumnsDefinitions,
        bool $truncateLargeColumns
    ): void {
        if (count($columns) === 0) {
            $query->addSelect('*');
            return;
        }

        foreach ($columns as $column) {
            $selectColumnExpression = BigqueryQuote::quoteSingleIdentifier($column);

            if ($truncateLargeColumns) {
                /** @var BigqueryColumn[] $def */
                $def = array_values(array_filter(
                    iterator_to_array($tableColumnsDefinitions),
                    fn(BigqueryColumn|ColumnInterface $c) => $c->getColumnName() === $column
                ));
                if (count($def) === 0) {
                    throw new QueryBuilderException(sprintf('Column "%s" not found in table definition.', $column));
                }
                $this->processSelectWithLargeColumnTruncation(
                    $query,
                    $selectColumnExpression,
                    $column,
                    $def[0]->getColumnDefinition()
                );
                continue;
            }
            $query->addSelect($selectColumnExpression);
        }
    }

    private function processSelectWithLargeColumnTruncation(
        QueryBuilder $query,
        string $selectColumnExpresion,
        string $column,
        Bigquery $def
    ): void {
        if ($def->getBasetype() === BaseType::STRING) {
            $this->addSelectLargeString($query, $selectColumnExpresion, $column);
            return;
        }
        if (in_array($def->getType(), [Bigquery::TYPE_TIME, Bigquery::TYPE_TIMESTAMP, Bigquery::TYPE_DATETIME], true)) {
            //don't cast time types
            //leave casting format to BQ
            $query->addSelect(
                sprintf(
                    '%s AS %s',
                    $selectColumnExpresion,
                    BigqueryQuote::quoteSingleIdentifier($column)
                )
            );
        } else {
            //cast value to string
            $query->addSelect(
                sprintf(
                    'CAST(%s as STRING) AS %s',
                    $selectColumnExpresion,
                    BigqueryQuote::quoteSingleIdentifier($column)
                )
            );
        }

        //flag not casted
        $query->addSelect(
            sprintf(
                '0 AS %s',
                BigqueryQuote::quoteSingleIdentifier(uniqid($column))
            )
        );
    }

    protected function processLimitStatement(int $limit, QueryBuilder $query): void
    {
        if ($limit > 0) {
            $query->setMaxResults($limit);
        }
    }
}
