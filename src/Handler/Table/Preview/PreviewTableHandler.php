<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Preview;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\NullValue;
use Google\Protobuf\Value;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Table\ImportExportShared\DataType;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableResponse;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

class PreviewTableHandler implements DriverCommandHandlerInterface
{
    public const STRING_MAX_LENGTH = 50;

    public const MAX_LIMIT = 1000;

    public const ALLOWED_DATA_TYPES = [
        DataType::INTEGER => Bigquery::TYPE_INTEGER,
        DataType::BIGINT => Bigquery::TYPE_BIGINT,
    ];

    private GCPClientManager $manager;

    public function __construct(GCPClientManager $manager)
    {
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
        /** @var string $databaseName */
        $databaseName = $command->getPath()[0];

        // build sql
        $columns = ProtobufHelper::repeatedStringToArray($command->getColumns());
        assert($columns === array_unique($columns), 'PreviewTableCommand.columns has non unique names');
        $columnsSql = implode(', ', array_map([BigqueryQuote::class, 'quoteSingleIdentifier'], $columns));

        // TODO changeSince, changeUntil
        // TODO fulltextSearch
        // TODO whereFilters
        // TODO truncated: rewrite to SQL
        $selectTableSql = sprintf(
            "SELECT %s\nFROM %s.%s",
            $columnsSql,
            BigqueryQuote::quoteSingleIdentifier($databaseName),
            BigqueryQuote::quoteSingleIdentifier($command->getTableName())
        );

        if ($command->getOrderBy()->count()) {
            $orderByParts = [];
            /**
             * @var int $index
             * @var PreviewTableCommand\PreviewTableOrderBy $orderBy
             */
            foreach ($command->getOrderBy() as $index => $orderBy) {
                assert($orderBy->getColumnName() !== '', sprintf(
                    'PreviewTableCommand.orderBy.%d.columnName is required',
                    $index,
                ));
                $quotedColumnName = BigqueryQuote::quoteSingleIdentifier($orderBy->getColumnName());
                $orderByParts[] = sprintf(
                    '%s %s',
                    $this->applyDataType($quotedColumnName, $orderBy->getDataType()),
                    $orderBy->getOrder() === PreviewTableCommand\PreviewTableOrderBy\Order::DESC ? 'DESC' : 'ASC'
                );
            }
            $selectTableSql .= sprintf(
                "\nORDER BY %s",
                implode(', ', $orderByParts),
            );
        }

        $selectTableSql .= sprintf(
            ' LIMIT %d',
            ($command->getLimit() > 0 && $command->getLimit() < self::MAX_LIMIT)
                ? $command->getLimit()
                : self::MAX_LIMIT
        );
        // select table
        $result = $bqClient->runQuery($bqClient->query($selectTableSql));

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

    private function applyDataType(string $columnName, int $dataType): string
    {
        if ($dataType === DataType::STRING) {
            return $columnName;
        }
        if (!array_key_exists($dataType, self::ALLOWED_DATA_TYPES)) {
            $allowedTypesList = [];
            foreach (self::ALLOWED_DATA_TYPES as $typeId => $typeName) {
                $allowedTypesList[] = sprintf('%s for %s', $typeId, $typeName);
            }
            throw new Exception(
                sprintf(
                    'Data type %s not recognized. Possible datatypes are [%s]',
                    $dataType,
                    implode('|', $allowedTypesList)
                )
            );
        }
        return sprintf(
            'CAST(%s AS %s)',
            $columnName,
            self::ALLOWED_DATA_TYPES[$dataType]
        );
    }
}
