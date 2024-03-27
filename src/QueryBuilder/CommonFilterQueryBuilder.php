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
        ColumnConverter $columnConverter,
    ) {
        $this->bigQueryClient = $bigQueryClient;
        $this->columnConverter = $columnConverter;
    }

    private function addSelectLargeString(QueryBuilder $query, string $selectColumn, string $column): void
    {
        //casted value
        $query->addSelect(
            sprintf(
                'SUBSTRING(CAST(%s as STRING), 0, %d) AS %s',
                $selectColumn,
                self::DEFAULT_CAST_SIZE,
                BigqueryQuote::quoteSingleIdentifier($column),
            ),
        );
        //flag if is cast
        $query->addSelect(
            sprintf(
                '(CASE WHEN LENGTH(CAST(%s as STRING)) > %s THEN 1 ELSE 0 END) AS %s',
                $selectColumn,
                self::DEFAULT_CAST_SIZE,
                BigqueryQuote::quoteSingleIdentifier(uniqid($column)),
            ),
        );
    }

    private function convertNonStringValue(
        TableWhereFilter $filter,
        string $value,
        QueryBuilder $query,
        ?string $realDatatype = null,
    ): string {
        if ($realDatatype === null) {
            switch (true) {
                case $filter->getDataType() === DataType::INTEGER:
                case $filter->getDataType() === DataType::BIGINT:
                    $realDatatype = Bigquery::TYPE_BIGINT;
                    break;
                case $filter->getDataType() === DataType::REAL:
                case $filter->getDataType() === DataType::DECIMAL:
                case $filter->getDataType() === DataType::DOUBLE:
                    $realDatatype = Bigquery::TYPE_FLOAT64;
                    break;
            }
        }
        return sprintf(
            'SAFE_CAST(%s AS %s)',
            $query->createNamedParameter($value, $filter->getDataType()),
            $realDatatype,
        );
    }

    protected function processChangedConditions(
        string $tableName,
        string $changeSince,
        string $changeUntil,
        QueryBuilder $query,
    ): void {
        if ($changeSince !== '') {
            $query->andWhere(sprintf(
                '%s.`_timestamp` >= :changedSince',
                BigqueryQuote::quoteSingleIdentifier($tableName),
            ));
            $query->setParameter(
                'changedSince',
                $this->getTimestampFormatted($changeSince),
            );
        }

        if ($changeUntil !== '') {
            $query->andWhere(sprintf(
                '%s.`_timestamp` < :changedUntil',
                BigqueryQuote::quoteSingleIdentifier($tableName),
            ));
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
    protected function processWhereFilters(
        RepeatedField $filters,
        QueryBuilder $query,
        string $tableName,
        ColumnCollection $tableColumnsDefinitions,
    ): void {
        $columnDefByName = [];
        foreach ($tableColumnsDefinitions as $column) {
            $columnDefByName[$column->getColumnName()] = $column;
        }

        foreach ($filters as $whereFilter) {
            $colDefinition = $columnDefByName[$whereFilter->getColumnsName()]->getColumnDefinition();
            $columnBaseType = $colDefinition->getBasetype();
            if ($whereFilter->getDataType() === DataType::STRING) {
                // default dataType but base type might be not string
                $convertedDatatype = match ($columnBaseType) {
                    BaseType::INTEGER => DataType::INTEGER,
                    BaseType::FLOAT, BaseType::NUMERIC => DataType::DOUBLE,
                    default => DataType::STRING,
                };
                $whereFilter->setDataType($convertedDatatype);
            }

            $values = ProtobufHelper::repeatedStringToArray($whereFilter->getValues());
            if (count($values) === 1) {
                $this->processSimpleValue(
                    $whereFilter,
                    reset($values),
                    $query,
                    $tableName,
                    $columnBaseType,
                    $colDefinition->getType(),
                );
            } else {
                $this->processMultipleValue(
                    $tableName,
                    $whereFilter,
                    $values,
                    $query,
                    $columnBaseType,
                    $colDefinition->getType(),
                );
            }
        }
    }

    private function processSimpleValue(
        TableWhereFilter $filter,
        string $value,
        QueryBuilder $query,
        string $tableName,
        string $baseType,
        string $realDatatype,
    ): void {
        // Scenarios:
        //
        // 1. baseType = STRING; realDatatype = *NOT_STRING
        // SQL -> SAFE_CAST(column as datatype) = SAFE_CAST('3.14' as dataType)
        // 2. baseType = STRING; realDatatype = STRING
        // SQL -> column = '3.14'
        // 3. baseType = *NOT_STRING; realDatatype = (doesn't matter)
        // SQL -> column = SAFE_CAST('3.14' as $realDatatype)
        //
        // *NOT_STRING = any type but STRING
        if ($baseType !== BaseType::STRING) {
            // 3
            $columnSql = sprintf(
                '%s.%s',
                BigqueryQuote::quoteSingleIdentifier($tableName),
                BigqueryQuote::quoteSingleIdentifier($filter->getColumnsName()),
            );
            $value = $this->convertNonStringValue($filter, $value, $query, $realDatatype);
        } elseif ($filter->getDataType() !== DataType::STRING) {
            // && $baseType === BaseType::STRING
            // 1
            $columnSql = $this->columnConverter->convertColumnByDataType(
                $tableName,
                $filter->getColumnsName(),
                $filter->getDataType(),
            );
            $value = $this->convertNonStringValue($filter, $value, $query);
        } else {
            // 2
            $columnSql = sprintf(
                '%s.%s',
                BigqueryQuote::quoteSingleIdentifier($tableName),
                BigqueryQuote::quoteSingleIdentifier($filter->getColumnsName()),
            );
        }

        $query->andWhere(
            sprintf(
                '%s %s %s',
                $columnSql,
                self::OPERATOR_SINGLE_VALUE[$filter->getOperator()],
                $value,
            ),
        );
    }

    /**
     * @param string[] $values
     */
    private function processMultipleValue(
        string $tableName,
        TableWhereFilter $filter,
        array $values,
        QueryBuilder $query,
        string $baseType,
        string $realDatatype,
    ): void {
        if (!array_key_exists($filter->getOperator(), self::OPERATOR_MULTI_VALUE)) {
            throw new QueryBuilderException(
                'whereFilter with multiple values can be used only with "eq", "ne" operators',
            );
        }

        if ($baseType !== BaseType::STRING) {
            // 3
            $columnSql = sprintf(
                '%s.%s',
                BigqueryQuote::quoteSingleIdentifier($tableName),
                BigqueryQuote::quoteSingleIdentifier($filter->getColumnsName()),
            );
            $values = array_map(
                fn(string $value) => $this->convertNonStringValue($filter, $value, $query, $realDatatype),
                $values,
            );
            $param = implode(', ', $values);
        } elseif ($filter->getDataType() !== DataType::STRING) {
            $columnSql = $this->columnConverter->convertColumnByDataType(
                $tableName,
                $filter->getColumnsName(),
                $filter->getDataType(),
            );
            $values = array_map(fn(string $value) => $this->convertNonStringValue($filter, $value, $query), $values);
            $param = implode(', ', $values);
        } else {
            $columnSql = sprintf(
                '%s.%s',
                BigqueryQuote::quoteSingleIdentifier($tableName),
                BigqueryQuote::quoteSingleIdentifier($filter->getColumnsName()),
            );
            $param = $query->createNamedParameter($values, Connection::PARAM_STR_ARRAY);
        }

        $query->andWhere(
            sprintf(
                '%s %s UNNEST(%s)',
                $columnSql,
                self::OPERATOR_MULTI_VALUE[$filter->getOperator()],
                $param,
            ),
        );
    }

    /**
     * @param RepeatedField|ExportOrderBy[] $sort
     */
    protected function processOrderStatement(string $tableName, RepeatedField $sort, QueryBuilder $query): void
    {
        try {
            foreach ($sort as $orderBy) {
                if ($orderBy->getDataType() !== DataType::STRING) {
                    $query->addOrderBy(
                        $this->columnConverter->convertColumnByDataType(
                            $tableName,
                            $orderBy->getColumnName(),
                            $orderBy->getDataType(),
                        ),
                        ExportOrderBy\Order::name($orderBy->getOrder()),
                    );
                    return;
                }
                $query->addOrderBy(
                    sprintf(
                        '%s.%s',
                        BigqueryQuote::quoteSingleIdentifier($tableName),
                        BigqueryQuote::quoteSingleIdentifier($orderBy->getColumnName()),
                    ),
                    ExportOrderBy\Order::name($orderBy->getOrder()),
                );
            }
        } catch (QueryException $e) {
            throw new QueryBuilderException(
                $e->getMessage(),
                $e,
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
        bool $truncateLargeColumns,
        string $tableName,
    ): void {
        if (count($columns) === 0) {
            $query->addSelect('*');
            return;
        }

        foreach ($columns as $column) {
            $selectColumn = sprintf(
                '%s.%s',
                BigqueryQuote::quoteSingleIdentifier($tableName),
                BigqueryQuote::quoteSingleIdentifier($column),
            );

            if ($truncateLargeColumns) {
                /** @var BigqueryColumn[] $def */
                $def = array_values(array_filter(
                    iterator_to_array($tableColumnsDefinitions),
                    fn(BigqueryColumn|ColumnInterface $c) => $c->getColumnName() === $column,
                ));
                if (count($def) === 0) {
                    throw new ColumnNotFoundException($column);
                }
                $this->processSelectWithLargeColumnTruncation(
                    $query,
                    $selectColumn,
                    $column,
                    $def[0]->getColumnDefinition(),
                );
                continue;
            }

            $query->addSelect($selectColumn);
        }
    }

    private function processSelectWithLargeColumnTruncation(
        QueryBuilder $query,
        string $selectColumn,
        string $column,
        Bigquery $def,
    ): void {
        if ($def->getType() === Bigquery::TYPE_ARRAY) {
            $query->addSelect(
                sprintf(
                    'IF(ARRAY_LENGTH(%s) = 0, NULL, SUBSTRING(TO_JSON_STRING(%s), 0, %d)) AS %s',
                    $selectColumn,
                    $selectColumn,
                    self::DEFAULT_CAST_SIZE,
                    BigqueryQuote::quoteSingleIdentifier($column),
                ),
            );

            //flag if is cast
            $query->addSelect(
                sprintf(
                    '(CASE WHEN LENGTH(TO_JSON_STRING(%s)) > %s THEN 1 ELSE 0 END) AS %s',
                    $selectColumn,
                    self::DEFAULT_CAST_SIZE,
                    BigqueryQuote::quoteSingleIdentifier(uniqid($column)),
                ),
            );
            return;
        }

        if ($def->getType() === Bigquery::TYPE_GEOGRAPHY) {
            $query->addSelect(
                sprintf(
                    'IF(%s IS NULL, NULL, SUBSTRING(ST_ASGEOJSON(%s), 0, %d)) AS %s',
                    $selectColumn,
                    $selectColumn,
                    self::DEFAULT_CAST_SIZE,
                    BigqueryQuote::quoteSingleIdentifier($column),
                ),
            );

            //flag if is cast
            $query->addSelect(
                sprintf(
                    '(CASE WHEN LENGTH(ST_ASGEOJSON(%s)) > %s THEN 1 ELSE 0 END) AS %s',
                    $selectColumn,
                    self::DEFAULT_CAST_SIZE,
                    BigqueryQuote::quoteSingleIdentifier(uniqid($column)),
                ),
            );
            return;
        }

        if (in_array($def->getType(), [Bigquery::TYPE_JSON, Bigquery::TYPE_STRUCT])) {
            $query->addSelect(
                sprintf(
                    'IF(%s IS NULL, NULL, SUBSTRING(TO_JSON_STRING(%s), 0, %d)) AS %s',
                    $selectColumn,
                    $selectColumn,
                    self::DEFAULT_CAST_SIZE,
                    BigqueryQuote::quoteSingleIdentifier($column),
                ),
            );

            //flag if is cast
            $query->addSelect(
                sprintf(
                    '(CASE WHEN LENGTH(TO_JSON_STRING(%s)) > %s THEN 1 ELSE 0 END) AS %s',
                    $selectColumn,
                    self::DEFAULT_CAST_SIZE,
                    BigqueryQuote::quoteSingleIdentifier(uniqid($column)),
                ),
            );

            return;
        }

        if ($def->getBasetype() === BaseType::STRING) {
            $this->addSelectLargeString($query, $selectColumn, $column);
            return;
        }
        if (in_array($def->getType(), [Bigquery::TYPE_TIME, Bigquery::TYPE_TIMESTAMP, Bigquery::TYPE_DATETIME], true)) {
            //don't cast time types
            //leave casting format to BQ
            $query->addSelect(
                sprintf(
                    '%s AS %s',
                    $selectColumn,
                    BigqueryQuote::quoteSingleIdentifier($column),
                ),
            );
        } else {
            //cast value to string
            $query->addSelect(
                sprintf(
                    'CAST(%s as STRING) AS %s',
                    $selectColumn,
                    BigqueryQuote::quoteSingleIdentifier($column),
                ),
            );
        }

        //flag not casted
        $query->addSelect(
            sprintf(
                '0 AS %s',
                BigqueryQuote::quoteSingleIdentifier(uniqid($column)),
            ),
        );
    }

    protected function processLimitStatement(int $limit, QueryBuilder $query): void
    {
        if ($limit > 0) {
            $query->setMaxResults($limit);
        }
    }
}
