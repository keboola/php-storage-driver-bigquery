<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use Exception;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\Billing\V1\ProjectBillingInfo;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Service\CloudResourceManager\Project;
use Google\Service\Exception as GoogleServiceException;
use Google_Service_Iam;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\CreateWorkspaceHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use LogicException;
use PHPUnit\Framework\TestCase;

class BaseCase extends TestCase
{
    protected GCPClientManager $clientManager;

    // to distinguish projects if you need more projects in one test case
    protected string $projectSuffix = '';

    /**
     * @param array<mixed> $data
     * @param int|string $dataName
     */
    public function __construct(?string $name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);
        $this->clientManager = new GCPClientManager();
    }

    /**
     * Get credentials from envs
     */
    protected function getCredentials(): GenericBackendCredentials
    {
        $principal = getenv('BQ_PRINCIPAL');
        if ($principal === false) {
            throw new LogicException('Env "BQ_PRINCIPAL" is empty');
        }

        $secret = getenv('BQ_SECRET');
        if ($secret === false) {
            throw new LogicException('Env "BQ_SECRET" is empty');
        }
        $secret = str_replace("\\n", "\n", $secret);

        $folderId = (string) getenv('BQ_FOLDER_ID');
        if ($folderId === '') {
            throw new LogicException('Env "BQ_FOLDER_ID" is empty');
        }

        $any = new Any();
        $any->pack((new GenericBackendCredentials\BigQueryCredentialsMeta())->setFolderId(
            $folderId
        ));
        return (new GenericBackendCredentials())
            ->setPrincipal($principal)
            ->setSecret($secret)
            ->setMeta($any);
    }

    protected function cleanTestProject(): void
    {
        $projectsClient = $this->clientManager->getProjectClient($this->getCredentials());
        $billingClient = $this->clientManager->getBillingClient($this->getCredentials());

        $meta = $this->getCredentials()->getMeta();
        if ($meta !== null) {
            // override root user and use other database as root
            $meta = $meta->unpack();
            assert($meta instanceof GenericBackendCredentials\BigQueryCredentialsMeta);
            $folderId = $meta->getFolderId();
        } else {
            throw new Exception('BigQueryCredentialsMeta is required.');
        }

        $parent = $folderId;
        // Iterate over pages of elements
        $pagedResponse = $projectsClient->listProjects('folders/' . $parent);
        foreach ($pagedResponse->iteratePages() as $page) {
            /** @var Project $element */
            foreach ($page as $element) {
                if (str_starts_with($element->getProjectId(), $this->getStackPrefix())) {
                    $formattedName = $projectsClient->projectName($element->getProjectId());
                    $billingInfo = new ProjectBillingInfo();
                    $billingInfo->setBillingEnabled(false);
                    $billingClient->updateProjectBillingInfo($formattedName, ['projectBillingInfo' => $billingInfo]);
                    $operationResponse = $projectsClient->deleteProject($formattedName);
                    $operationResponse->pollUntilComplete();
                    if (!$operationResponse->operationSucceeded()) {
                        $error = $operationResponse->getError();
                        assert($error !== null);
                        throw new Exception($error->getMessage(), $error->getCode());
                    }
                }
            }
        }
    }

    /**
     * @param array<mixed> $expected
     * @param array<mixed> $actual
     */
    protected function assertEqualsArrays(array $expected, array $actual): void
    {
        sort($expected);
        sort($actual);

        $this->assertEquals($expected, $actual);
    }

    protected function getStackPrefix(): string
    {
        $stackPrefix = getenv('BQ_STACK_PREFIX');
        if ($stackPrefix === false) {
            $stackPrefix = 'local';
        }
        return $stackPrefix;
    }

    protected function getProjectId(): string
    {
        return 'project-' . date('m-d-H-i-s') . $this->projectSuffix;
    }

    /**
     * @return array{GenericBackendCredentials, CreateProjectResponse}
     */
    protected function createTestProject(): array
    {
        $handler = new CreateProjectHandler($this->clientManager);
        $command = new CreateprojectCommand();

        $meta = new Any();
        $meta->pack((new CreateProjectCommand\CreateProjectBigqueryMeta())->setGcsFileBucketName(
            (string) getenv('BQ_BUCKET_NAME')
        ));
        $command->setStackPrefix($this->getStackPrefix());
        $command->setProjectId($this->getProjectId());
        $command->setMeta($meta);

        $response = $handler(
            $this->getCredentials(),
            $command,
            []
        );

        assert($response instanceof CreateProjectResponse);

        return [
            (new GenericBackendCredentials())
                ->setPrincipal($response->getProjectUserName())
                ->setSecret($response->getProjectPassword()),
            $response,
        ];
    }

    protected function createTestBucket(
        GenericBackendCredentials $projectCredentials
    ): CreateBucketResponse {
        $bucket = md5($this->getName()) . 'in.c-Test';

        $handler = new CreateBucketHandler($this->clientManager);
        $command = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($this->getProjectId())
            ->setBucketId($bucket);

        $response = $handler(
            $projectCredentials,
            $command,
            []
        );

        $this->assertInstanceOf(CreateBucketResponse::class, $response);

        $bigQueryClient = $this->clientManager->getBigQueryClient($projectCredentials);

        $dataset = $bigQueryClient->dataset($response->getCreateBucketObjectName());

        $bucketInfo = $dataset->info();
        $this->assertArrayNotHasKey('defaultTableExpirationMs', $bucketInfo);
        $this->assertInstanceOf(Dataset::class, $dataset);
        $this->assertEquals($response->getCreateBucketObjectName(), $dataset->identity()['datasetId']);
        $this->assertTrue($dataset->exists());
        return $response;
    }

    protected function getGCSClient(): StorageClient
    {
        return new StorageClient(['keyFile' => CredentialsHelper::getCredentialsArray($this->getCredentials())]);
    }

    protected function clearGCSBucketDir(string $bucket, string $prefix): void
    {
        $client = $this->getGCSClient();
        $bucket = $client->bucket($bucket);
        $objects = $bucket->objects(['prefix' => $prefix]);
        foreach ($objects as $object) {
            $object->delete();
        }
    }

    /**
     * @return StorageObject[]
     */
    protected function listGCSFiles(string $bucket, string $prefix): array
    {
        $client = $this->getGCSClient();
        $bucket = $client->bucket($bucket);
        $objects = $bucket->objects(['prefix' => $prefix]);
        return iterator_to_array($objects);
    }

    /**
     * @return array{size: int, files: string[]}
     */
    protected function listFilesSimple(string $bucket, string $prefix): array
    {
        /** @var array{size: int, files: string[]} $result */
        $result = array_reduce(
            $this->listGCSFiles($bucket, $prefix),
            static function (array $agg, StorageObject $file) {
                $agg['size'] += (int) $file->info()['size'];
                $agg['files'][] = $file->name();
                return $agg;
            },
            ['size' => 0, 'files' => []]
        );

        return $result;
    }

    protected function getWorkspaceId(): string
    {
        return substr(md5($this->getName()), 0, 9) . '_test';
    }

    /**
     * @return array{GenericBackendCredentials, CreateWorkspaceResponse}
     */
    protected function createTestWorkspace(
        GenericBackendCredentials $projectCredentials,
        CreateProjectResponse $projectResponse
    ): array {
        $handler = new CreateWorkspaceHandler($this->clientManager);
        $command = (new CreateWorkspaceCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setProjectId($this->getProjectId())
            ->setWorkspaceId($this->getWorkspaceId())
            ->setProjectReadOnlyRoleName($projectResponse->getProjectReadOnlyRoleName());
        $response = $handler(
            $projectCredentials,
            $command,
            []
        );
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $credentials = (new GenericBackendCredentials())
            ->setHost($projectCredentials->getHost())
            ->setPrincipal($response->getWorkspaceUserName())
            ->setSecret($response->getWorkspacePassword())
            ->setPort($projectCredentials->getPort());
        return [$credentials, $response];
    }

    /**
     * @param array{columns: array<string, array<string, mixed>>, primaryKeysNames: array<int, string>} $structure
     */
    protected function createTable(
        GenericBackendCredentials $credentials,
        string $databaseName,
        string $tableName,
        array $structure
    ): void {
        $createTableHandler = new CreateTableHandler($this->clientManager);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $databaseName;

        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        /** @var array{type: string, length: string, nullable: bool} $columnData */
        foreach ($structure['columns'] as $columnName => $columnData) {
            $columns[] = (new TableColumnShared)
                ->setName($columnName)
                ->setType($columnData['type'])
                ->setLength($columnData['length'])
                ->setNullable($columnData['nullable']);
        }

        $createTableCommand = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns);

        $createTableResponse = $createTableHandler(
            $credentials,
            $createTableCommand,
            []
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $createTableResponse);
        $this->assertSame(ObjectType::TABLE, $createTableResponse->getObjectType());
    }

    /**
     * @param array{columns: string, rows: array<int, string>}[] $insertGroups
     */
    protected function fillTableWithData(
        GenericBackendCredentials $credentials,
        string $databaseName,
        string $tableName,
        array $insertGroups
    ): void {
        $bqClient = $this->clientManager->getBigQueryClient($credentials);
        foreach ($insertGroups as $insertGroup) {
            foreach ($insertGroup['rows'] as $insertRow) {
                $insertSql = sprintf(
                    "INSERT INTO %s.%s\n(%s) VALUES\n(%s);",
                    BigqueryQuote::quoteSingleIdentifier($databaseName),
                    BigqueryQuote::quoteSingleIdentifier($tableName),
                    $insertGroup['columns'],
                    $insertRow
                );
                $inserted = $bqClient->runQuery($bqClient->query($insertSql));
                $this->assertEquals(1, $inserted->info()['numDmlAffectedRows']);
            }
        }
    }

    public function isTableExists(BigQueryClient $projectBqClient, string $datasetName, string $tableName): bool
    {
        $dataset = $projectBqClient->dataset($datasetName);
        $table = $dataset->table($tableName);
        return $table->exists();
    }

    public function isDatabaseExists(BigQueryClient $projectBqClient, string $datasetName): bool
    {
        $dataset = $projectBqClient->dataset($datasetName);
        return $dataset->exists();
    }

    public function isUserExists(Google_Service_Iam $iamService, string $workspacePublicCredentialsPart): bool
    {
        /** @var array<string, string> $credentialsArr */
        $credentialsArr = (array) json_decode($workspacePublicCredentialsPart, true, 512, JSON_THROW_ON_ERROR);
        $serviceAccountsService = $iamService->projects_serviceAccounts;

        try {
            $serviceAcc = $serviceAccountsService->get(
                sprintf(
                    'projects/%s/serviceAccounts/%s',
                    $credentialsArr['project_id'],
                    $credentialsArr['client_email']
                )
            );
        } catch (GoogleServiceException $e) {
            if ($e->getCode() === 404) {
                return false;
            } else {
                throw $e;
            }
        }

        assert($serviceAcc !== null);
        return true;
    }

    protected function createTestTable(
        GenericBackendCredentials $credentials,
        string $database,
        ?string $tableName = null
    ): string {
        if ($tableName === null) {
            $tableName = md5($this->getName()) . '_Test_table';
        }

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $database;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setType(Bigquery::TYPE_INTEGER);
        $columns[] = (new TableColumnShared)
            ->setName('name')
            ->setType(Bigquery::TYPE_STRING)
            ->setLength('50')
            ->setNullable(true)
            ->setDefault("'Some Default'");
        $columns[] = (new TableColumnShared)
            ->setName('large')
            ->setType(Bigquery::TYPE_STRING)
            ->setLength('10000');
        $primaryKeysNames = new RepeatedField(GPBType::STRING);
        $primaryKeysNames[] = 'id';
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns)
            ->setPrimaryKeysNames($primaryKeysNames);

        $handler(
            $credentials,
            $command,
            []
        );
        return $tableName;
    }
}
