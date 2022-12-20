<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Query\QueryException;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\QueryBuilder\FakeConnection\FakeConnectionFactory;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportFilters;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use LogicException;

class ExportQueryBuilder extends CommonFilterQueryBuilder
{
    public const DEFAULT_CAST_SIZE = 16384;

    private ?TableInfo $tableInfo;

    public function __construct(
        BigQueryClient $bqClient,
        ?TableInfo $tableInfo,
        ColumnConverter $columnConverter
    ) {
        $this->tableInfo = $tableInfo;

        parent::__construct($bqClient, $columnConverter);
    }

    /**
     * @throws QueryBuilderException
     */
    public function buildQueryFromCommand(
        ?ExportFilters $filters,
        RepeatedField $orderBy,
        RepeatedField $columns,
        string $schemaName,
        string $tableName
    ): QueryBuilderResponse {
        $query = new QueryBuilder(FakeConnectionFactory::getConnection());

        if ($filters !== null) {
            $this->assertFilterCombination($filters);
            $this->processFilters($filters, $query);
        }

        $this->processOrderStatement($orderBy, $query);
        $this->processSelectStatement(ProtobufHelper::repeatedStringToArray($columns), $query);
        $this->processFromStatement($schemaName, $tableName, $query);

        $sql = $query->getSQL();
        $params = $query->getParameters();
        // replace named parameters from DBAL to BQ style
        // WHERE _timestamp < :changeSince -> WHERE _timestamp < @changeSince
        foreach ($params as $key => $value) {
            $sql = str_replace(
                sprintf(':%s', $key),
                sprintf('@%s', $key),
                $sql
            );
        }

        /** @var string[] $types */
        $types = $query->getParameterTypes();

        return new QueryBuilderResponse(
            $sql,
            $params,
            $types,
        );
    }

    private function assertFilterCombination(ExportFilters $options): void
    {
        if ($options->getFulltextSearch() !== '' && $options->getWhereFilters()->count()) {
            throw new QueryBuilderException(
                'Cannot use fulltextSearch and whereFilters at the same time',
            );
        }
    }

    /**
     * @param string[] $columns
     */
    private function buildFulltextFilters(
        QueryBuilder $query,
        string $fulltextSearchKey,
        array $columns
    ): void {
        foreach ($columns as $column) {
            $query->orWhere(
                $query->expr()->like(
                    BigqueryQuote::quoteSingleIdentifier($column),
                    BigqueryQuote::quote("%{$fulltextSearchKey}%")
                )
            );
        }
    }

    private function getBasetype(string $type): string
    {
        return (new Bigquery($type))->getBasetype();
    }

    private function processFilters(ExportFilters $filters, QueryBuilder $query): void
    {
        $this->processChangedConditions($filters->getChangeSince(), $filters->getChangeUntil(), $query);
        try {
            if ($filters->getFulltextSearch() !== '') {
                if ($this->tableInfo === null) {
                    throw new LogicException('tableInfo variable has to be set to use fulltextSearch');
                }

                $tableInfoColumns = [];
                /** @var TableInfo\TableColumn $column */
                foreach ($this->tableInfo->getColumns() as $column) {
                    // search only in STRING types
                    if ($this->getBasetype($column->getType()) === BaseType::STRING) {
                        $tableInfoColumns[] = $column->getName();
                    }
                }

                $this->buildFulltextFilters(
                    $query,
                    $filters->getFulltextSearch(),
                    $tableInfoColumns,
                );
            } else {
                $this->processWhereFilters($filters->getWhereFilters(), $query);
            }
        } catch (QueryException $e) {
            throw new QueryBuilderException(
                $e->getMessage(),
                $e
            );
        }
        $this->processLimitStatement($filters->getLimit(), $query);
    }
}
