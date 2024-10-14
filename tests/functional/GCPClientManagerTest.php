<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests;

use Google\Protobuf\Any;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use LogicException;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class GCPClientManagerTest extends TestCase
{
    /**
     * Get credentials from envs
     */
    protected function getCredentials(string $region = BaseCase::DEFAULT_LOCATION): GenericBackendCredentials
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
        $any->pack(
            (new GenericBackendCredentials\BigQueryCredentialsMeta())
                ->setFolderId($folderId)
                ->setRegion($region),
        );
        return (new GenericBackendCredentials())
            ->setPrincipal($principal)
            ->setSecret($secret)
            ->setMeta($any);
    }

    public function testUserAgent(): void
    {
        $historyContainer = [];
        $historyMiddleware = Middleware::history($historyContainer);
        $handlerStack = HandlerStack::create();
        $handlerStack->push($historyMiddleware);
        $credentials = $this->getCredentials();
        $meta = new Any();
        $meta->pack(new GenericBackendCredentials\BigQueryCredentialsMeta());
        $credentials->setMeta($meta);
        $logger = new NullLogger();
        $GLOBALS['log'] = $logger;
        $connection = (new GCPClientManager($logger))->getBigQueryClient(
            '123',
            $credentials,
            $handlerStack,
        );
        $query = $connection->query('SELECT 1');
        $connection->runQuery($query);

        $this->assertNotEmpty($historyContainer, 'No requests were captured.');
        foreach ($historyContainer as $transaction) {
            /** @var Request $request */
            $request = $transaction['request'];
            $headers = $request->getHeaders();

            $this->assertArrayHasKey('User-Agent', $headers, 'User-Agent header is missing.');
            $this->assertEquals(
                'Keboola/1.0 (GPN:Keboola; connection)',
                $headers['User-Agent'][0],
                'User-Agent header is incorrect.',
            );
        }
    }
}
