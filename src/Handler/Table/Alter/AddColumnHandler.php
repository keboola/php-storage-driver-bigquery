<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Table\Alter;

use Google\Protobuf\Internal\Message;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Table\AddColumnCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

final class AddColumnHandler extends BaseHandler
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
     * @param AddColumnCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof AddColumnCommand);

        assert($runtimeOptions->getMeta() === null);

        $column = $command->getColumnDefinition();
        // validate
        assert($command->getPath()->count() === 1, 'AddColumnCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'AddColumnCommand.tableName is required');
        assert($column instanceof TableColumnShared, 'AddColumnCommand.columnDefinition is required');

        assert($column->getNullable() === false, 'You cannot add a REQUIRED column to an existing table schema.');
        assert($column->getDefault() === '', 'You cannot add a DEFAULT to column an existing table schema.');
        $bqClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);

        // define columns
        // validate
        assert($column->getName() !== '', 'TableColumnShared.name is required');
        assert($column->getType() !== '', 'TableColumnShared.type is required');

        $columnDefinition = new BigqueryColumn(
            $column->getName(),
            new Bigquery($column->getType(), [
                'length' => $column->getLength() === '' ? null : $column->getLength(),
                'nullable' => true,
                'default' => null,
            ])
        );

        // build sql
        $builder = new BigqueryTableQueryBuilder();
        /** @var string $databaseName */
        $databaseName = $command->getPath()[0];
        $createTableSql = $builder->getAddColumnCommand(
            $databaseName,
            $command->getTableName(),
            $columnDefinition
        );

        $bqClient->runQuery($bqClient->query($createTableSql));

        return (new ObjectInfoResponse())
            ->setPath($command->getPath())
            ->setObjectType(ObjectType::TABLE)
            ->setTableInfo(TableReflectionResponseTransformer::transformTableReflectionToResponse(
                $databaseName,
                new BigqueryTableReflection(
                    $bqClient,
                    $databaseName,
                    $command->getTableName()
                )
            ));
    }
}
