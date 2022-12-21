<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Preview;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Info\ObjectInfoHandler;
use Keboola\StorageDriver\BigQuery\QueryBuilder\ExportQueryBuilderFactory;
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

class PreviewTableHandler implements DriverCommandHandlerInterface
{
    public const STRING_MAX_LENGTH = 50;

    public const DEFAULT_LIMIT = 100;
    public const MAX_LIMIT = 1000;

    private GCPClientManager $manager;

    private ExportQueryBuilderFactory $queryBuilderFactory;


    public function __construct(
        GCPClientManager $manager,
        ExportQueryBuilderFactory $queryBuilderFactory
    ) {
        $this->manager = $manager;
        $this->queryBuilderFactory = $queryBuilderFactory;
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
        $tableInfo = $this->getTableInfoResponseIfNeeded($credentials, $command, $datasetName);
        $queryBuilder = $this->queryBuilderFactory->create($bqClient, $tableInfo);
        $queryData = $queryBuilder->buildQueryFromCommand(
            $command->getFilters(),
            $command->getOrderBy(),
            $command->getColumns(),
            $datasetName,
            $command->getTableName()
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
        /** @var array<string, string> $line */
        foreach ($result->getIterator() as $line) {
            // set row
            $row = new PreviewTableResponse\Row();
            $rowColumns = new RepeatedField(GPBType::MESSAGE, PreviewTableResponse\Row\Column::class);
            /** @var ?scalar $itemValue */
            foreach ($line as $itemKey => $itemValue) {
                // set row columns
                $value = new Value();
                $truncated = false;
                if ($itemValue === null) {
                    $value->setNullValue(NullValue::NULL_VALUE);
                } else {
                    // preview returns all data as string
                    // TODO truncated: rewrite to SQL
                    if (mb_strlen((string) $itemValue) > self::STRING_MAX_LENGTH) {
                        $truncated = true;
                        $value->setStringValue(mb_substr((string) $itemValue, 0, self::STRING_MAX_LENGTH));
                    } else {
                        $value->setStringValue((string) $itemValue);
                    }
                }

                $rowColumns[] = (new PreviewTableResponse\Row\Column())
                    ->setColumnName($itemKey)
                    ->setValue($value)
                    ->setIsTruncated($truncated);

                $row->setColumns($rowColumns);
            }
            $rows[] = $row;
        }
        $response->setRows($rows);
        return $response;
    }

    /**
     * fulltext search need table info data
     */
    private function getTableInfoResponseIfNeeded(
        GenericBackendCredentials $credentials,
        PreviewTableCommand $command,
        string $databaseName
    ): ?TableInfo {
        if ($command->getFilters() !== null && $command->getFilters()->getFulltextSearch() !== '') {
            $objectInfoHandler = (new ObjectInfoHandler($this->manager));
            $tableInfoCommand = (new ObjectInfoCommand())
                ->setExpectedObjectType(ObjectType::TABLE)
                ->setPath(ProtobufHelper::arrayToRepeatedString([
                    $databaseName,
                    $command->getTableName(),
                ]));
            /** @var ObjectInfoResponse $response */
            $response = $objectInfoHandler($credentials, $tableInfoCommand, []);

            assert($response instanceof ObjectInfoResponse);
            assert($response->getObjectType() === ObjectType::TABLE);

            return $response->getTableInfo();
        }
        return null;
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
