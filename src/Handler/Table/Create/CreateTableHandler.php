<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Create;

use Google\Cloud\Core\Exception\BadRequestException;
use Google\Protobuf\Internal\Message;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\Helper\CreateTableMetaHelper;
use Keboola\StorageDriver\BigQuery\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\SQLtoRestDatatypeConverter;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Throwable;

final class CreateTableHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateTableCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateTableCommand);

        // validate
        assert($command->getPath()->count() === 1, 'CreateTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'CreateTableCommand.tableName is required');
        assert($command->getColumns()->count() > 0, 'CreateTableCommand.columns is required');

        /** @var string $datasetName */
        $datasetName = $command->getPath()[0];

        // define columns
        $createTableOptions = [
            'schema' => [
                'fields' => [],
            ],
            'retries' => self::DEFAULT_RETRY_OVERRIDE,
        ];
        /** @var TableColumnShared $column */
        foreach ($command->getColumns() as $column) {
            // validate
            assert($column->getName() !== '', 'TableColumnShared.name is required');
            assert($column->getType() !== '', 'TableColumnShared.type is required');

            $columnDefinition = new Bigquery($column->getType(), [
                'length' => $column->getLength() === '' ? null : $column->getLength(),
                'nullable' => $column->getNullable(),
                'default' => $column->getDefault() === '' ? null : $column->getDefault(),
            ]);
            try {
                $createTableOptions['schema']['fields'][] = SQLtoRestDatatypeConverter::convertColumnToRestFormat(
                    new BigqueryColumn($column->getName(), $columnDefinition),
                );
            } catch (InvalidLengthException $e) {
                BadTableDefinitionException::handleInvalidLengthException(
                    $e,
                    $datasetName,
                    $command->getTableName(),
                );
            }
        }

        if ($command->getPrimaryKeysNames()->count() > 0) {
            $createTableOptions['tableConstraints'] = [
                'primaryKey' => [
                    'columns' => ProtobufHelper::repeatedStringToArray($command->getPrimaryKeysNames()),
                ],
            ];
        }

        $createTableOptions = array_merge(
            $createTableOptions,
            CreateTableMetaHelper::convertTableMetaToRest($command),
        );

        $bqClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);
        if ($runtimeOptions->getRunId() !== '') {
            $createTableOptions['labels'] = ['run_id' => $runtimeOptions->getRunId(),];
        }

        $dataset = $bqClient->dataset($datasetName);

        try {
            $dataset->createTable(
                $command->getTableName(),
                $createTableOptions,
            );
        } catch (BadRequestException $e) {
            BadTableDefinitionException::handleBadRequestException(
                $e,
                $datasetName,
                $command->getTableName(),
                $createTableOptions,
            );
        }

        try {
            $tableInfo = TableReflectionResponseTransformer::transformTableReflectionToResponse(
                $datasetName,
                new BigqueryTableReflection(
                    $bqClient,
                    $datasetName,
                    $command->getTableName(),
                ),
            );
        } catch (Throwable $e) {
            // if something goes wrong in reflection queries, delete the table
            $dataset->table($command->getTableName())->delete();
            throw $e;
        }

        return (new ObjectInfoResponse())
            ->setPath($command->getPath())
            ->setObjectType(ObjectType::TABLE)
            ->setTableInfo($tableInfo);
    }
}
