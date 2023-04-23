<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Preview;

use DateTime;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Info\ObjectInfoHandler;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ColumnConverter;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ExportQueryBuilder;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

class PreviewTableHandler implements DriverCommandHandlerInterface
{
    public const DEFAULT_LIMIT = 100;
    public const MAX_LIMIT = 1000;

    private GCPClientManager $manager;

    public function __construct(
        GCPClientManager $manager
    ) {
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
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof PreviewTableCommand);

        // validate
        assert($command->getPath()->count() === 1, 'PreviewTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'PreviewTableCommand.tableName is required');
        assert($command->getColumns()->count() > 0, 'PreviewTableCommand.columns is required');

        $bqClient = $this->manager->getBigQueryClient($credentials);
        /** @var string $datasetName */
        $datasetName = $command->getPath()[0];

        $this->validateFilters($command);

        // build sql
        $queryBuilder = new ExportQueryBuilder($bqClient, new ColumnConverter());
        $tableColumnsDefinitions = (new BigqueryTableReflection($bqClient, $datasetName, $command->getTableName()))
            ->getColumnsDefinitions();
        $queryData = $queryBuilder->buildQueryFromCommand(
            ExportQueryBuilder::MODE_SELECT,
            $command->getFilters(),
            $command->getOrderBy(),
            $command->getColumns(),
            $tableColumnsDefinitions,
            $datasetName,
            $command->getTableName(),
            true
        );

        /** @var array<string> $queryDataBindings */
        $queryDataBindings = $queryData->getBindings();

        // select table
        $result = $bqClient->runQuery(
            $bqClient->query($queryData->getQuery())
                ->parameters($queryDataBindings)
        );

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
                        $columnValue = $columnValue->format('Y-m-d H:i:s');
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

    private function validateFilters(PreviewTableCommand $command): void
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
                    'PreviewTableCommand.changeSince must be numeric timestamp'
                );
            }
            if ($filters->getChangeUntil() !== '') {
                assert(
                    is_numeric($filters->getChangeUntil()),
                    'PreviewTableCommand.changeUntil must be numeric timestamp'
                );
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
}
