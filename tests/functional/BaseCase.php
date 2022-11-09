<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use Exception;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\Billing\V1\ProjectBillingInfo;
use Google\Protobuf\Any;
use Google\Service\CloudResourceManager\Project;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\UseCase\Bucket\CreateDropBucketTest;
use LogicException;
use PHPUnit\Framework\TestCase;

class BaseCase extends TestCase
{
    protected GCPClientManager $clientManager;

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
        return 'project-' . date('m-d-H-i-s');
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
}
