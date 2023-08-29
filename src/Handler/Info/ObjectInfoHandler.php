<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Info;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\Core\Exception\NotFoundException;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\Command\Info\DatabaseInfo;
use Keboola\StorageDriver\Command\Info\ObjectInfo;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\SchemaInfo;
use Keboola\StorageDriver\Command\Info\ViewInfo;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\ObjectNotFoundException;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\UnknownObjectException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Schema\Bigquery\BigquerySchemaReflection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;

final class ObjectInfoHandler implements DriverCommandHandlerInterface
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof ObjectInfoCommand);
        assert($runtimeOptions->getRunId() === '');
        assert($runtimeOptions->getMeta() === null);

        $bqClient = $this->clientManager->getBigQueryClient($credentials);

        $path = ProtobufHelper::repeatedStringToArray($command->getPath());

        assert(count($path) !== 0, 'Error empty path.');

        $response = (new ObjectInfoResponse())
            ->setPath($command->getPath())
            ->setObjectType($command->getExpectedObjectType());

        switch ($command->getExpectedObjectType()) {
            // DATABASE === PROJECT in BQ
            case ObjectType::DATABASE:
                return $this->getDatabaseResponse($path, $bqClient, $response);
            case ObjectType::SCHEMA:
                return $this->getSchemaResponse($path, $bqClient, $response);
            case ObjectType::VIEW:
                return $this->getViewResponse($path, $bqClient, $response);
            case ObjectType::TABLE:
                return $this->getTableResponse($path, $response, $bqClient);
            default:
                throw new UnknownObjectException(ObjectType::name($command->getExpectedObjectType()));
        }
    }

    /**
     * @return Generator<int, ObjectInfo>
     */
    private function getChildSchemas(BigQueryClient $bqClient): Generator
    {
        $datasets = $bqClient->datasets();
        /** @var Dataset $child */
        foreach ($datasets as $child) {
            yield (new ObjectInfo())
                ->setObjectType(ObjectType::SCHEMA)
                ->setObjectName($child->info()['datasetReference']['datasetId']);
        }
    }

    /**
     * @return Generator<int, ObjectInfo>
     */
    private function getChildObjectsForSchema(BigQueryClient $bqClient, string $databaseName): Generator
    {
        $ref = new BigquerySchemaReflection($bqClient, $databaseName);
        $tables = $ref->getTablesNames();
        foreach ($tables as $table) {
            yield (new ObjectInfo())
                ->setObjectType(ObjectType::TABLE)
                ->setObjectName($table);
        }
        $views = $ref->getViewsNames();
        foreach ($views as $view) {
            yield (new ObjectInfo())
                ->setObjectType(ObjectType::VIEW)
                ->setObjectName($view);
        }
    }

    /**
     * @param string[] $path
     */
    private function getDatabaseResponse(
        array $path,
        BigQueryClient $bqClient,
        ObjectInfoResponse $response
    ): ObjectInfoResponse {
        assert(count($path) === 1, 'Error path must have exactly one element.');
        $objects = new RepeatedField(GPBType::MESSAGE, ObjectInfo::class);
        foreach ($this->getChildSchemas($bqClient) as $object) {
            $objects[] = $object;
        }

        $infoObject = new DatabaseInfo();
        $infoObject->setObjects($objects);

        $response->setDatabaseInfo($infoObject);

        return $response;
    }

    /**
     * @param string[] $path
     */
    private function getSchemaResponse(
        array $path,
        BigQueryClient $bqClient,
        ObjectInfoResponse $response
    ): ObjectInfoResponse {
        assert(count($path) === 1, 'Error path must have exactly one element.');
        $infoObject = new SchemaInfo();
        $objects = new RepeatedField(GPBType::MESSAGE, ObjectInfo::class);
        $this->assertDatasetExist($bqClient, $path[0]);
        foreach ($this->getChildObjectsForSchema($bqClient, $path[0]) as $object) {
            $objects[] = $object;
        }
        $this->assertSchemaExists($objects, $bqClient, $path[0]);
        $infoObject->setObjects($objects);
        $response->setSchemaInfo($infoObject);
        return $response;
    }

    /**
     * @param string[] $path
     */
    private function getTableResponse(
        array $path,
        ObjectInfoResponse $response,
        BigQueryClient $bqClient
    ): ObjectInfoResponse {
        assert(count($path) === 2, 'Error path must have exactly two elements.');
        $this->assertDatasetExist($bqClient, $path[0]);
        try {
            $response->setTableInfo(TableReflectionResponseTransformer::transformTableReflectionToResponse(
                $path[0],
                new BigqueryTableReflection(
                    $bqClient,
                    $path[0],
                    $path[1]
                )
            ));
        } catch (TableNotExistsReflectionException $e) {
            throw new ObjectNotFoundException($path[1]);
        }
        return $response;
    }

    /**
     * @param string[] $path
     */
    private function getViewResponse(
        array $path,
        BigQueryClient $bqClient,
        ObjectInfoResponse $response
    ): ObjectInfoResponse {
        assert(count($path) === 2, 'Error path must have exactly two elements.');
        $this->assertDatasetExist($bqClient, $path[0]);
        $infoObject = new ViewInfo();
        // todo: set view props
        $response->setViewInfo($infoObject);
        return $response;
    }

    private function assertSchemaExists(RepeatedField $objects, BigQueryClient $bqClient, string $databaseName): void
    {
        if ($objects->count() === 0) {
            // test if database exists
            $datasetSchema = $bqClient->dataset($databaseName);
            if ($datasetSchema->exists() === false) {
                throw new ObjectNotFoundException($databaseName);
            }
        }
    }

    private function assertDatasetExist(BigQueryClient $bqClient, string $datasetName): void
    {
        $datasetSchema = $bqClient->dataset($datasetName);
        if ($datasetSchema->exists() === false) {
            throw new ObjectNotFoundException($datasetName);
        }
    }
}
