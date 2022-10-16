<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use Exception;
use Google\Protobuf\Any;
use Google\Service\CloudResourceManager\Project;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
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
                $exploded = explode('-', $element->getProjectId());
                if ($exploded[0] === $this->getStackPrefix()) {
                    $formattedName = $projectsClient->projectName($element->getProjectId());
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
            throw new LogicException('Env "BQ_STACK_PREFIX" is empty');
        }
        return $stackPrefix;
    }

    protected function getProjectId(): string
    {
        return 'project-' . date('m-d-H-i-s');
    }
}
