<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use DateTime;
use DateTimeZone;
use Doctrine\DBAL\Query\QueryBuilder;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\OrderBy\Order;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter\Operator;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

abstract class CommonFilterQueryBuilder
{
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

    protected function processChangedConditions(string $changeSince, string $changeUntil, QueryBuilder $query): void
    {
        if ($changeSince !== '') {
            $query->andWhere('"_timestamp" >= :changedSince');
            $query->setParameter(
                'changedSince',
                $this->getTimestampFormatted($changeSince),
            );
        }

        if ($changeUntil !== '') {
            $query->andWhere('"_timestamp" < :changedUntil');
            $query->setParameter(
                'changedUntil',
                $this->getTimestampFormatted($changeUntil),
            );
        }
    }

    /**
     * @param string $timestamp
     */
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
            switch (true) {
                case $filter->getDataType() === DataType::INTEGER:
                    $value = (int) $value;
                    break;
                case $filter->getDataType() === DataType::REAL:
                    $value = (float) $value;
                    break;
            }
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
        } else {
            $columnSql = BigqueryQuote::quoteSingleIdentifier($filter->getColumnsName());
        }

        $quotedValues = array_map(static fn(string $value) => BigqueryQuote::quote($value), $values);

        $query->andWhere(
            sprintf(
                '%s %s (%s)',
                $columnSql,
                self::OPERATOR_MULTI_VALUE[$filter->getOperator()],
                implode(',', $quotedValues)
            )
        );
    }

    /**
     * @param RepeatedField|OrderBy[] $sort
     */
    protected function processOrderStatement(RepeatedField $sort, QueryBuilder $query): void
    {
        foreach ($sort as $orderBy) {
            if ($orderBy->getDataType() !== DataType::STRING) {
                $query->addOrderBy(
                    $this->columnConverter->convertColumnByDataType($orderBy->getColumnName(), $orderBy->getDataType()),
                    Order::name($orderBy->getOrder())
                );
                return;
            }
            $query->addOrderBy(
                BigqueryQuote::quoteSingleIdentifier($orderBy->getColumnName()),
                Order::name($orderBy->getOrder())
            );
        }
    }

    /**
     * @param string[] $columns
     */
    protected function processSelectStatement(array $columns, QueryBuilder $query): void
    {
        if (count($columns) === 0) {
            $query->addSelect('*');
            return;
        }

        foreach ($columns as $column) {
            $selectColumnExpresion = BigqueryQuote::quoteSingleIdentifier($column);

            // TODO truncate - preview does not contains export format
            //if ($options->shouldTruncateLargeColumns()) {
            //    $this->processSelectWithLargeColumnTruncation($query, $selectColumnExpresion, $column);
            //    return;
            //}
            $query->addSelect($selectColumnExpresion);
        }
    }

    // TODO truncate - preview does not contains export format
    /*private function processSelectWithLargeColumnTruncation(
        QueryBuilder $query,
        string $selectColumnExpresion,
        string $column
    ): void {
        //casted value
        $query->addSelect(
            sprintf(
                'CAST(SUBSTRING(%s, 0, %d) as VARCHAR(%d)) AS %s',
                $selectColumnExpresion,
                self::DEFAULT_CAST_SIZE,
                self::DEFAULT_CAST_SIZE,
                BigqueryQuote::quoteSingleIdentifier($column)
            )
        );
        //flag if is cast
        $query->addSelect(
            sprintf(
                '(IF LENGTH(%s) > %s THEN 1 ELSE 0 ENDIF) AS %s',
                BigqueryQuote::quoteSingleIdentifier($column),
                self::DEFAULT_CAST_SIZE,
                BigqueryQuote::quoteSingleIdentifier(uniqid($column))
            )
        );
    }*/

    protected function processLimitStatement(int $limit, QueryBuilder $query): void
    {
        if ($limit > 0) {
            $query->setMaxResults($limit);
        }
    }

    protected function processFromStatement(string $schemaName, string $tableName, QueryBuilder $query): void
    {
        $query->from(sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($schemaName),
            BigqueryQuote::quoteSingleIdentifier($tableName)
        ));
    }
}
