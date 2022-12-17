<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Query\QueryException;
use Keboola\StorageDriver\BigQuery\QueryBuilder\FakeConnection\FakeConnectionFactory;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOptions;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use LogicException;

class TableExportFilterQueryBuilder extends CommonFilterQueryBuilder
{
    public function buildQueryFromCommand(
        TableExportToFileCommand $command,
        string $schemaName,
        string $tableName
    ): QueryBuilderResponse {
        $options = $command->getExportOptions() ?? new ExportOptions();

        $query = new QueryBuilder(FakeConnectionFactory::getConnection());

        $this->processChangedConditions($options->getChangeSince(), $options->getChangeUntil(), $query);

        try {
            $this->processWhereFilters($options->getWhereFilters(), $query);

            $this->processOrderStatement($options->getOrderBy(), $query);
        } catch (QueryException $e) {
            throw new QueryBuilderException(
                $e->getMessage(),
                $e
            );
        }

        $this->processSelectStatement(ProtobufHelper::repeatedStringToArray($options->getColumnsToExport()), $query);
        $this->processLimitStatement($options->getLimit(), $query);
        $this->processFromStatement($schemaName, $tableName, $query);

        $sql = $query->getSQL();

        /** @var string[] $types */
        $types = $query->getParameterTypes();

        return new QueryBuilderResponse(
            $sql,
            $query->getParameters(),
            $types,
            ProtobufHelper::repeatedStringToArray($options->getColumnsToExport()),
        );
    }
}
