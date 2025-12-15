<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use Doctrine\DBAL\Query\QueryBuilder;
use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\StorageDriver\BigQuery\QueryBuilder\FakeConnection\FakeConnectionFactory;
use Keboola\StorageDriver\Command\Workspace\LoadTableToWorkspaceCommand;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;

final class WorkspaceLoadQueryBuilder extends CommonFilterQueryBuilder
{
    public function __construct(BigQueryClient $bqClient, ColumnConverter $columnConverter)
    {
        parent::__construct($bqClient, $columnConverter);
    }

    /**
     * Builds SELECT SQL for workspace load operations.
     *
     * Note: LoadTableToWorkspaceCommand doesn't support WHERE filters, LIMIT, or time travel (seconds).
     * This is a simplified version compared to TableImportQueryBuilder.
     *
     * @param string[] $selectColumns
     * @throws QueryBuilderException
     */
    public function buildSelectSourceSql(
        BigqueryTableDefinition $sourceDefinition,
        array $selectColumns,
        LoadTableToWorkspaceCommand\SourceTableMapping $sourceMapping,
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

        // Note: WHERE filters, LIMIT, and time travel (seconds) are not supported in workspace loads
        // These features are only available in table imports via TableImportQueryBuilder

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
