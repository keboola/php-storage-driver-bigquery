<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Create;

use Google\Protobuf\Internal\Message;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\CreateTableCommand\TableColumn;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

final class CreateTableHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
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
        array $features
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateTableCommand);

        // validate
        assert($command->getPath()->count() === 1, 'CreateTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'CreateTableCommand.tableName is required');
        assert($command->getColumns()->count() > 0, 'CreateTableCommand.columns is required');

        // define columns
        $columns = [];
        /** @var TableColumn $column */
        foreach ($command->getColumns() as $column) {
            // validate
            assert($column->getName() !== '', 'TableColumn.name is required');
            assert($column->getType() !== '', 'TableColumn.type is required');

            $columnDefinition = new Bigquery($column->getType(), [
                'length' => $column->getLength() === '' ? null : $column->getLength(),
                'nullable' => $column->getNullable(),
                'default' => $column->getDefault() === '' ? null : $column->getDefault(),
            ]);
            $columns[] = new BigqueryColumn($column->getName(), $columnDefinition);
        }
        $columnsCollection = new ColumnCollection($columns);

        $builder = new BigqueryTableQueryBuilder();
        /** @var string $datasetName */
        $datasetName = $command->getPath()[0];
        $bqClient = $this->clientManager->getBigQueryClient($credentials);
        $dataset = $bqClient->dataset($datasetName);

        $createTableSql = $builder->getCreateTableCommand(
            $dataset->id(),
            $command->getTableName(),
            $columnsCollection,
            [] // primary keys aren't supported in BQ
        );

        $query = $bqClient->query($createTableSql);
        $bqClient->runQuery($query);

        return (new ObjectInfoResponse())
            ->setPath($command->getPath())
            ->setObjectType(ObjectType::TABLE)
            ->setTableInfo(TableReflectionResponseTransformer::transformTableReflectionToResponse(
                $datasetName,
                new BigqueryTableReflection(
                    $bqClient,
                    $datasetName,
                    $command->getTableName()
                )
            ));
    }
}

