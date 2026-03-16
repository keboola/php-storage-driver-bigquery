<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class CreateViewQueryBuilder
{
    /**
     * @param string[] $columns
     * @param RepeatedField|TableWhereFilter[] $whereFilters
     */
    public function buildCreateViewSql(
        string $datasetName,
        string $viewName,
        string $sourceDatasetName,
        string $sourceTableName,
        array $columns,
        RepeatedField $whereFilters,
    ): string {
        $selectExpression = $this->buildSelectExpression($columns);
        $whereClause = $this->buildWhereClause($whereFilters);

        return sprintf(
            'CREATE OR REPLACE VIEW %s.%s AS (SELECT %s FROM %s.%s%s)',
            BigqueryQuote::quoteSingleIdentifier($datasetName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
            $selectExpression,
            BigqueryQuote::quoteSingleIdentifier($sourceDatasetName),
            BigqueryQuote::quoteSingleIdentifier($sourceTableName),
            $whereClause,
        );
    }

    /**
     * @param string[] $columns
     */
    private function buildSelectExpression(array $columns): string
    {
        foreach ($columns as $col) {
            if ($col === '') {
                throw new QueryBuilderException('Column name must not be empty.');
            }
        }

        if (count($columns) === 0) {
            return '*';
        }

        return implode(', ', array_map(
            static fn(string $col): string => BigqueryQuote::quoteSingleIdentifier($col),
            $columns,
        ));
    }

    /**
     * @param RepeatedField|TableWhereFilter[] $whereFilters
     */
    private function buildWhereClause(RepeatedField $whereFilters): string
    {
        $whereClauses = [];
        /** @var TableWhereFilter $filter */
        foreach ($whereFilters as $filter) {
            $column = BigqueryQuote::quoteSingleIdentifier($filter->getColumnsName());
            /** @var string[] $values */
            $values = iterator_to_array($filter->getValues());
            $operator = $filter->getOperator();

            if (count($values) === 0) {
                throw new QueryBuilderException(sprintf(
                    'WHERE filter for column "%s" must have at least one value.',
                    $filter->getColumnsName(),
                ));
            }

            if (count($values) === 1) {
                $sqlOperator = CommonFilterQueryBuilder::OPERATOR_SINGLE_VALUE[$operator]
                    ?? throw new QueryBuilderException(sprintf('Unsupported operator: %d', $operator));
                $whereClauses[] = sprintf('%s %s %s', $column, $sqlOperator, BigqueryQuote::quote($values[0]));
            } else {
                $sqlOperator = CommonFilterQueryBuilder::OPERATOR_MULTI_VALUE[$operator]
                    ?? throw new QueryBuilderException(sprintf(
                        'Operator %d does not support multiple values.',
                        $operator,
                    ));
                $quotedValues = implode(', ', array_map(
                    static fn(string $v): string => BigqueryQuote::quote($v),
                    $values,
                ));
                $whereClauses[] = sprintf('%s %s (%s)', $column, $sqlOperator, $quotedValues);
            }
        }

        if (count($whereClauses) === 0) {
            return '';
        }

        return ' WHERE ' . implode(' AND ', $whereClauses);
    }
}
