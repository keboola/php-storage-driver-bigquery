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
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportFilters;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use LogicException;

class ExportQueryBuilder extends CommonFilterQueryBuilder
{
    public const MODE_SELECT = 'SELECT';
    public const MODE_DELETE = 'DELETE';
    public const MODE_PREVIEW = 'PREVIEW';

    public function __construct(
        BigQueryClient $bqClient,
        ColumnConverter $columnConverter,
    ) {
        parent::__construct($bqClient, $columnConverter);
    }

    /**
     * @param self::MODE_* $mode
     * @throws QueryBuilderException
     * @throws ColumnNotFoundException
     */
    public function buildQueryFromCommand(
        string $mode,
        ?ExportFilters $filters,
        RepeatedField $orderBy,
        RepeatedField $columns,
        ColumnCollection $tableColumnsDefinitions,
        string $schemaName,
        string $tableName,
        bool $truncateLargeColumns,
        int $rowsCount = 0,
    ): QueryBuilderResponse {
        $query = new QueryBuilder(FakeConnectionFactory::getConnection());

        $limit = 0;
        if ($filters !== null) {
            $this->assertFilterCombination($filters);
            $this->processFilters(
                $filters,
                $query,
                $tableColumnsDefinitions,
                $tableName,
            );
            $limit = $filters->getLimit();
        }

        $from = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($schemaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        );
        switch ($mode) {
            case self::MODE_PREVIEW:
            case self::MODE_SELECT:
                $this->processOrderStatement($tableName, $orderBy, $query);
                $this->processSelectStatement(
                    ProtobufHelper::repeatedStringToArray($columns),
                    $query,
                    $tableColumnsDefinitions,
                    $truncateLargeColumns,
                    $tableName,
                );
                if ($mode === self::MODE_PREVIEW && !$this->isFilterUsed($filters) && $rowsCount > 10_000) {
                    // do not take sample from small tables under 10000 rows
                    $defaultSample = 10; // 10% sample over 10000 rows is 1000 rows which si limit in storage
                    if ($limit !== 0 && $rowsCount / 100 > $limit) {
                        // 1% of rows in table is higher than limit, we will sample only 1%
                        // default limit in storage is 100 rows so this will be almost always true
                        $defaultSample = 1; // 1% is minimal sample
                    }
                    $from .= sprintf(' TABLESAMPLE SYSTEM (%d PERCENT)', $defaultSample);
                }
                $query->from($from);
                break;
            case self::MODE_DELETE:
                $query->delete($from);
                break;
            default:
                throw new LogicException(sprintf(
                    'Unknown mode "%s".',
                    $mode,
                ));
        }

        $sql = $query->getSQL();
        $params = $query->getParameters();
        // replace named parameters from DBAL to BQ style
        // WHERE _timestamp < :changeSince -> WHERE _timestamp < @changeSince
        foreach ($params as $key => $value) {
            $sql = str_replace(
                sprintf(':%s', $key),
                sprintf('@%s', $key),
                $sql,
            );
        }

        return new QueryBuilderResponse(
            $sql,
            $params,
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
        array $columns,
        string $tableName,
    ): void {
        foreach ($columns as $column) {
            $query->orWhere(
                $query->expr()->like(
                    sprintf(
                        '%s.%s',
                        BigqueryQuote::quoteSingleIdentifier($tableName),
                        BigqueryQuote::quoteSingleIdentifier($column),
                    ),
                    BigqueryQuote::quote("%{$fulltextSearchKey}%"),
                ),
            );
        }
    }

    private function getBasetype(string $type): string
    {
        return (new Bigquery($type))->getBasetype();
    }

    private function processFilters(
        ExportFilters $filters,
        QueryBuilder $query,
        ColumnCollection $tableColumnsDefinitions,
        string $tableName,
    ): void {
        $this->processChangedConditions($tableName, $filters->getChangeSince(), $filters->getChangeUntil(), $query);
        try {
            if ($filters->getFulltextSearch() !== '') {
                $tableInfoColumns = [];
                /** @var BigqueryColumn $column */
                foreach ($tableColumnsDefinitions as $column) {
                    // search only in STRING types
                    if ($this->getBasetype($column->getColumnDefinition()->getType()) === BaseType::STRING) {
                        $tableInfoColumns[] = $column->getColumnName();
                    }
                }

                $this->buildFulltextFilters(
                    $query,
                    $filters->getFulltextSearch(),
                    $tableInfoColumns,
                    $tableName,
                );
            } else {
                $this->processWhereFilters($filters->getWhereFilters(), $query, $tableName, $tableColumnsDefinitions);
            }
        } catch (QueryException $e) {
            throw new QueryBuilderException(
                $e->getMessage(),
                $e,
            );
        }
        $this->processLimitStatement($filters->getLimit(), $query);
    }

    private function isFilterUsed(?ExportFilters $filters): bool
    {
        return $filters !== null && (
                $filters->getChangeSince() !== ''
                || $filters->getChangeUntil() !== ''
                || $filters->getFulltextSearch() !== ''
                || $filters->getWhereFilters()->count() > 0
            );
    }
}
