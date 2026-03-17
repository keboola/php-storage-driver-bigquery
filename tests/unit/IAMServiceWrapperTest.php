<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests;

use Google\Service\Iam\Resource\ProjectsServiceAccounts;
use Google\Service\Iam\Resource\ProjectsServiceAccountsKeys;
use Google\Service\Resource;
use Google_Client;
use Keboola\StorageDriver\BigQuery\IAMServiceWrapper;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;

class IAMServiceWrapperTest extends TestCase
{
    private IAMServiceWrapper $wrapper;

    protected function setUp(): void
    {
        parent::setUp();
        $client = new Google_Client();
        $this->wrapper = new IAMServiceWrapper($client);
    }

    public function testServiceAccountsResourceIsRegistered(): void
    {
        $this->assertInstanceOf(
            ProjectsServiceAccounts::class,
            $this->wrapper->projects_serviceAccounts,
        );
    }

    public function testServiceAccountsKeysResourceIsRegistered(): void
    {
        $this->assertInstanceOf(
            ProjectsServiceAccountsKeys::class,
            $this->wrapper->projects_serviceAccounts_keys,
        );
    }

    public function testServiceAccountsMethodsUseV1Paths(): void
    {
        $methods = $this->getResourceMethods($this->wrapper->projects_serviceAccounts);

        $this->assertArrayHasKey('create', $methods);
        $this->assertArrayHasKey('delete', $methods);
        $this->assertArrayHasKey('get', $methods);
        $this->assertArrayHasKey('list', $methods);

        $this->assertSame('v1/{+name}/serviceAccounts', $methods['create']['path']);
        $this->assertSame('POST', $methods['create']['httpMethod']);

        $this->assertSame('v1/{+name}', $methods['delete']['path']);
        $this->assertSame('DELETE', $methods['delete']['httpMethod']);

        $this->assertSame('v1/{+name}', $methods['get']['path']);
        $this->assertSame('GET', $methods['get']['httpMethod']);

        $this->assertSame('v1/{+name}/serviceAccounts', $methods['list']['path']);
        $this->assertSame('GET', $methods['list']['httpMethod']);
    }

    public function testServiceAccountsKeysMethodsUseV1Paths(): void
    {
        $methods = $this->getResourceMethods($this->wrapper->projects_serviceAccounts_keys);

        $this->assertArrayHasKey('create', $methods);
        $this->assertArrayHasKey('delete', $methods);
        $this->assertArrayHasKey('list', $methods);

        $this->assertSame('v1/{+name}/keys', $methods['create']['path']);
        $this->assertSame('POST', $methods['create']['httpMethod']);

        $this->assertSame('v1/{+name}', $methods['delete']['path']);
        $this->assertSame('DELETE', $methods['delete']['httpMethod']);

        $this->assertSame('v1/{+name}/keys', $methods['list']['path']);
        $this->assertSame('GET', $methods['list']['httpMethod']);
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function getResourceMethods(object $resource): array
    {
        $ref = new ReflectionProperty(Resource::class, 'methods');
        $ref->setAccessible(true);
        $methods = $ref->getValue($resource);
        assert(is_array($methods));
        /** @var array<string, array<string, mixed>> $methods */
        return $methods;
    }
}
