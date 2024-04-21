<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Preview;

use DateTime;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Common;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\BadExportFilterParametersException;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ExportQueryBuilder;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Command\Table\ImportExportShared\TableWhereFilter;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

final class PreviewTableHandler extends BaseHandler
{
    public const DEFAULT_LIMIT = 100;
    public const MAX_LIMIT = 1000;

    /** @var array<int, Bigquery::TYPE_*> */
    public const TYPES_UNSUPPORTED_IN_FILTERS = [
        Bigquery::TYPE_ARRAY,
        Bigquery::TYPE_STRUCT,
        Bigquery::TYPE_BYTES,
        Bigquery::TYPE_GEOGRAPHY,
        Bigquery::TYPE_INTERVAL,
        Bigquery::TYPE_JSON,
    ];

    private GCPClientManager $manager;

    public function __construct(
        GCPClientManager $manager,
    ) {
        parent::__construct();
        $this->manager = $manager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param PreviewTableCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof PreviewTableCommand);

        assert($runtimeOptions->getMeta() === null);

        // validate
        assert($command->getPath()->count() === 1, 'PreviewTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'PreviewTableCommand.tableName is required');
        assert($command->getColumns()->count() > 0, 'PreviewTableCommand.columns is required');

        $bqClient = $this->manager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);
        /** @var string $datasetName */
        $datasetName = $command->getPath()[0];

        $tableColumnsDefinitions = (new BigqueryTableReflection($bqClient, $datasetName, $command->getTableName()))
            ->getColumnsDefinitions();
        $this->validateFilters($command, $tableColumnsDefinitions);

        // build sql
        $queryBuilder = new ExportQueryBuilder($bqClient, new ColumnConverter());
        $queryData = $queryBuilder->buildQueryFromCommand(
            ExportQueryBuilder::MODE_SELECT,
            $command->getFilters(),
            $command->getOrderBy(),
            $command->getColumns(),
            $tableColumnsDefinitions,
            $datasetName,
            $command->getTableName(),
            true,
        );

        /** @var array<string> $queryDataBindings */
        $queryDataBindings = $queryData->getBindings();

        // select table
        try {
            $result = $bqClient->executeQuery(
                $bqClient->query($queryData->getQuery())
                    ->parameters($queryDataBindings),
            );
        } catch (BadRequestException $e) {
            BadExportFilterParametersException::handleWrongTypeInFilters($e);
            throw $e;
        }

        // set response
        $response = new PreviewTableResponse();

        // set column names
        $response->setColumns($command->getColumns());

        // set rows
        $rows = new RepeatedField(GPBType::MESSAGE, PreviewTableResponse\Row::class);
        /** @var array<string, string|int> $row */
        foreach ($result->getIterator() as $row) {
            $responseRow = new PreviewTableResponse\Row();
            $responseRowColumns = new RepeatedField(GPBType::MESSAGE, PreviewTableResponse\Row\Column::class);
            /** @var string $columnName */
            foreach ($command->getColumns() as $columnName) {
                $value = new Value();
                /** @var null|bool|float|int|string|DateTime $columnValue */
                $columnValue = array_shift($row);
                if ($columnValue === null) {
                    $value->setNullValue(NullValue::NULL_VALUE);
                } else {
                    if ($columnValue instanceof DateTime) {
                        $columnValue = $columnValue->format('Y-m-d H:i:s.u');
                    }
                    $value->setStringValue((string) $columnValue);
                }

                $responseRowColumns[] = (new PreviewTableResponse\Row\Column())
                    ->setColumnName($columnName)
                    ->setValue($value)
                    ->setIsTruncated(array_shift($row) === 1);
            }
            $responseRow->setColumns($responseRowColumns);
            $rows[] = $responseRow;
        }
        $response->setRows($rows);
        return $response;
    }

    private function validateFilters(PreviewTableCommand $command, ColumnCollection $tableColumnsDefinitions): void
    {
        // build sql
        $columns = ProtobufHelper::repeatedStringToArray($command->getColumns());
        assert($columns === array_unique($columns), 'PreviewTableCommand.columns has non unique names');

        $filters = $command->getFilters();
        if ($filters !== null) {
            assert($filters->getLimit() <= self::MAX_LIMIT, 'PreviewTableCommand.limit cannot be greater than 1000');
            if ($filters->getLimit() === 0) {
                $filters->setLimit(self::DEFAULT_LIMIT);
            }

            if ($filters->getChangeSince() !== '') {
                assert(
                    is_numeric($filters->getChangeSince()),
                    'PreviewTableCommand.changeSince must be numeric timestamp',
                );
            }
            if ($filters->getChangeUntil() !== '') {
                assert(
                    is_numeric($filters->getChangeUntil()),
                    'PreviewTableCommand.changeUntil must be numeric timestamp',
                );
            }

            if ($filters->getWhereFilters() !== null) {
                $this->assertBackendSupportedFilterTypes($command, $tableColumnsDefinitions);
            }
        }

        /**
         * @var int $index
         * @var ExportOrderBy $orderBy
         */
        foreach ($command->getOrderBy() as $index => $orderBy) {
            assert($orderBy->getColumnName() !== '', sprintf(
                'PreviewTableCommand.orderBy.%d.columnName is required',
                $index,
            ));
        }
    }

    private function assertBackendSupportedFilterTypes(
        PreviewTableCommand $command,
        ColumnCollection $tableColumnsDefinitions,
    ): void {
        $whereFilters = $command->getFilters()?->getWhereFilters();
        if ($whereFilters === null || count($whereFilters) === 0) {
            return;
        }

        /** @var TableWhereFilter $filter */
        foreach ($whereFilters as $filter) {
            foreach ($tableColumnsDefinitions as $col) {
                /** @var Common $columnDefinition */
                $columnDefinition = $col->getColumnDefinition();
                $isSameCol = $filter->getColumnsName() === $col->getColumnName();
                if ($isSameCol && in_array($columnDefinition->getType(), self::TYPES_UNSUPPORTED_IN_FILTERS, true)) {
                    throw BadExportFilterParametersException::createUnsupportedDatatypeInWhereFilter(
                        $filter->getColumnsName(),
                        $columnDefinition->getType(),
                    );
                }
            }
        }
    }
}
