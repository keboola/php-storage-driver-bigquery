<?php

namespace Keboola\StorageDriver\BigQuery\QueryBuilder;

use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Query\QueryException;
use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\QueryBuilder\FakeConnection\FakeConnectionFactory;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use LogicException;

class TablePreviewFilterQueryBuilder extends CommonFilterQueryBuilder
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

    public function buildQueryFromCommand(
        PreviewTableCommand $options,
        string $schemaName
    ): QueryBuilderResponse {
        $this->assertFilterCombination($options);

        $query = new QueryBuilder(FakeConnectionFactory::getConnection());

        $this->processChangedConditions($options->getChangeSince(), $options->getChangeUntil(), $query);

        try {
            if ($options->getFulltextSearch() !== '') {
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
                    $options->getFulltextSearch(),
                    $tableInfoColumns,
                );
            } else {
                $this->processWhereFilters($options->getWhereFilters(), $query);
            }

            $this->processOrderStatement($options->getOrderBy(), $query);
        } catch (QueryException $e) {
            throw new QueryBuilderException(
                $e->getMessage(),
                $e
            );
        }

        $this->processSelectStatement(ProtobufHelper::repeatedStringToArray($options->getColumns()), $query);
        $this->processLimitStatement($options->getLimit(), $query);
        $this->processFromStatement($schemaName, $options->getTableName(), $query);

        $sql = $query->getSQL();

        /** @var string[] $types */
        $types = $query->getParameterTypes();

        return new QueryBuilderResponse(
            $sql,
            $query->getParameters(),
            $types,
            ProtobufHelper::repeatedStringToArray($options->getColumns()),
        );
    }

    private function assertFilterCombination(PreviewTableCommand $options): void
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
        $platform = $this->connection->getDatabasePlatform();
        assert($platform !== null);
        foreach ($columns as $column) {
            $query->orWhere(
                $query->expr()->like(
                    BigqueryQuote::quoteSingleIdentifier($column),
                    $platform->quoteStringLiteral("%{$fulltextSearchKey}%")
                )
            );
        }
    }

    private function getBasetype(string $type): string
    {
        return (new Bigquery($type))->getBasetype();
    }
}
