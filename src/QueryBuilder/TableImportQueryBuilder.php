<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use Doctrine\DBAL\Query\QueryBuilder;
use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\StorageDriver\BigQuery\QueryBuilder\FakeConnection\FakeConnectionFactory;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;

final class TableImportQueryBuilder extends CommonFilterQueryBuilder
{
    public function __construct(BigQueryClient $bqClient, ColumnConverter $columnConverter)
    {
        parent::__construct($bqClient, $columnConverter);
    }

    /**
     * @param string[] $selectColumns
     * @throws QueryBuilderException
     */
    public function buildSelectSourceSql(
        BigqueryTableDefinition $sourceDefinition,
        array $selectColumns,
        TableImportFromTableCommand\SourceTableMapping $sourceMapping,
    ): QueryBuilderResponse {
        $query = new QueryBuilder(FakeConnectionFactory::getConnection());
        $tableName = $sourceDefinition->getTableName();
        $schemaName = $sourceDefinition->getSchemaName();

        if ($selectColumns === []) {
            $query->addSelect(sprintf('%s.*', BigqueryQuote::quoteSingleIdentifier($tableName)));
        } else {
            foreach ($selectColumns as $column) {
                $query->addSelect(sprintf(
                    '%s.%s',
                    BigqueryQuote::quoteSingleIdentifier($tableName),
                    BigqueryQuote::quoteSingleIdentifier($column),
                ));
            }
        }

        $from = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($schemaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        );
        $query->from($from);

        $seconds = (int) $sourceMapping->getSeconds();
        if ($seconds > 0) {
            $query->andWhere(sprintf(
                '%s.`_timestamp` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d SECOND)',
                BigqueryQuote::quoteSingleIdentifier($tableName),
                $seconds,
            ));
        }

        $whereFilters = $sourceMapping->getWhereFilters();
        if ($whereFilters !== null && $whereFilters->count() > 0) {
            $this->processWhereFilters(
                $whereFilters,
                $query,
                $tableName,
                $sourceDefinition->getColumnsDefinitions(),
            );
        }

        $limit = (int) $sourceMapping->getLimit();
        if ($limit > 0) {
            $query->setMaxResults($limit);
        }

        $sql = $query->getSQL();
        $params = $query->getParameters();
        foreach ($params as $key => $value) {
            $sql = str_replace(
                sprintf(':%s', $key),
                sprintf('@%s', $key),
                $sql,
            );
        }

        return new QueryBuilderResponse($sql, $params);
    }
}
