<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateViewHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\CreateViewCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Throwable;

class CreateViewTest extends BaseCase
{
    private const SOURCE_TABLE_NAME = 'SOURCE_TABLE';

    protected CreateBucketResponse $bucketResponse;

    private GenericBackendCredentials $projectCredentials;

    private string $bucketDatasetName;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];
        $this->bucketResponse = $this->createTestBucket($this->projectCredentials);
        $this->bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        $this->createSourceTable();
    }

    private function createSourceTable(): void
    {
        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $this->bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared())
            ->setName('ID')
            ->setType(Bigquery::TYPE_STRING);
        $columns[] = (new TableColumnShared())
            ->setName('NAME')
            ->setType(Bigquery::TYPE_STRING);
        $columns[] = (new TableColumnShared())
            ->setName('AGE')
            ->setType(Bigquery::TYPE_STRING);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName(self::SOURCE_TABLE_NAME)
            ->setColumns($columns);
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bqClient->runQuery($bqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`, `NAME`, `AGE`) VALUES (%s, %s, %s), (%s, %s, %s)',
            BigqueryQuote::quoteSingleIdentifier($this->bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier(self::SOURCE_TABLE_NAME),
            BigqueryQuote::quote('1'),
            BigqueryQuote::quote('alice'),
            BigqueryQuote::quote('25'),
            BigqueryQuote::quote('2'),
            BigqueryQuote::quote('bob'),
            BigqueryQuote::quote('30'),
        )));
    }

    public function testCreateViewAllColumns(): void
    {
        $viewName = $this->getTestHash() . '_VIEW_ALL';

        $handler = new CreateViewHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $this->bucketDatasetName;
        $command = (new CreateViewCommand())
            ->setPath($path)
            ->setViewName($viewName)
            ->setSourceTableName(self::SOURCE_TABLE_NAME);

        $result = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertNull($result);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $view = $bqClient->dataset($this->bucketDatasetName)->table($viewName);
        $this->assertTrue($view->exists(), 'VIEW should exist in dataset');

        $rows = iterator_to_array($bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($this->bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
        ))));
        $this->assertCount(2, $rows);

        // Cleanup
        $dropHandler = new DropTableHandler($this->clientManager);
        $dropHandler->setInternalLogger($this->log);
        $dropHandler(
            $this->projectCredentials,
            (new DropTableCommand())
                ->setPath($path)
                ->setTableName($viewName),
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    public function testCreateViewColumnSubset(): void
    {
        $viewName = $this->getTestHash() . '_VIEW_SUBSET';

        $handler = new CreateViewHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $this->bucketDatasetName;

        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'ID';
        $columns[] = 'NAME';

        $command = (new CreateViewCommand())
            ->setPath($path)
            ->setViewName($viewName)
            ->setSourceTableName(self::SOURCE_TABLE_NAME)
            ->setColumns($columns);

        $result = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertNull($result);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $rows = iterator_to_array($bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($this->bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
        ))));

        $this->assertCount(2, $rows);
        // Verify only ID and NAME columns are returned
        /** @var array<string, mixed> $firstRow */
        $firstRow = $rows[0];
        $this->assertArrayHasKey('ID', $firstRow);
        $this->assertArrayHasKey('NAME', $firstRow);
        $this->assertArrayNotHasKey('AGE', $firstRow);

        // Cleanup
        $dropHandler = new DropTableHandler($this->clientManager);
        $dropHandler->setInternalLogger($this->log);
        $dropHandler(
            $this->projectCredentials,
            (new DropTableCommand())
                ->setPath($path)
                ->setTableName($viewName),
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    public function testCreateOrReplaceView(): void
    {
        $viewName = $this->getTestHash() . '_VIEW_REPLACE';

        $handler = new CreateViewHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $this->bucketDatasetName;

        // First: create VIEW with all columns
        $command = (new CreateViewCommand())
            ->setPath($path)
            ->setViewName($viewName)
            ->setSourceTableName(self::SOURCE_TABLE_NAME);

        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // Verify all columns
        $rows = iterator_to_array($bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($this->bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
        ))));
        $this->assertCount(2, $rows);
        /** @var array<string, mixed> $firstRow */
        $firstRow = $rows[0];
        $this->assertArrayHasKey('AGE', $firstRow);

        // Second: replace VIEW with column subset -- should succeed (OR REPLACE)
        $columns = new RepeatedField(GPBType::STRING);
        $columns[] = 'ID';
        $columns[] = 'NAME';

        $replaceCommand = (new CreateViewCommand())
            ->setPath($path)
            ->setViewName($viewName)
            ->setSourceTableName(self::SOURCE_TABLE_NAME)
            ->setColumns($columns);

        $handler(
            $this->projectCredentials,
            $replaceCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // Verify replaced VIEW has only ID and NAME
        $rows = iterator_to_array($bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($this->bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
        ))));
        $this->assertCount(2, $rows);
        /** @var array<string, mixed> $firstRow */
        $firstRow = $rows[0];
        $this->assertArrayHasKey('ID', $firstRow);
        $this->assertArrayHasKey('NAME', $firstRow);
        $this->assertArrayNotHasKey('AGE', $firstRow);

        // Cleanup
        $dropHandler = new DropTableHandler($this->clientManager);
        $dropHandler->setInternalLogger($this->log);
        $dropHandler(
            $this->projectCredentials,
            (new DropTableCommand())
                ->setPath($path)
                ->setTableName($viewName),
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    public function testCreateCrossDatasetViewAddsAuthorizedView(): void
    {
        // Create second bucket (dataset B) in same project
        $bucketBId = $this->getTestHash() . 'in.c-TestB';
        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $expectedBbDatasetName = $nameGenerator->createObjectNameForBucketInProject($bucketBId, null);

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        // Cleanup stale dataset B from previous runs
        try {
            $staleBbDataset = $bqClient->dataset($expectedBbDatasetName);
            if ($staleBbDataset->exists()) {
                $staleBbDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }

        $bbHandler = new CreateBucketHandler($this->clientManager);
        $bbHandler->setInternalLogger($this->log);
        $bbCommand = (new CreateBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setBucketId($bucketBId);
        $bbMeta = new Any();
        $bbMeta->pack(new CreateBucketCommand\CreateBucketBigqueryMeta());
        $bbCommand->setMeta($bbMeta);
        /** @var CreateBucketResponse $bbResponse */
        $bbResponse = $bbHandler(
            $this->projectCredentials,
            $bbCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $bbResponse);
        $bucketBDatasetName = $bbResponse->getCreateBucketObjectName();

        // Create cross-dataset VIEW in dataset B referencing table in dataset A
        $viewName = $this->getTestHash() . '_CROSS_VIEW';
        $handler = new CreateViewHandler($this->clientManager);
        $handler->setInternalLogger($this->log);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketBDatasetName;
        $sourcePath = new RepeatedField(GPBType::STRING);
        $sourcePath[] = $this->bucketDatasetName;

        $command = (new CreateViewCommand())
            ->setPath($path)
            ->setSourcePath($sourcePath)
            ->setViewName($viewName)
            ->setSourceTableName(self::SOURCE_TABLE_NAME);

        $result = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertNull($result);

        // Verify VIEW works
        $view = $bqClient->dataset($bucketBDatasetName)->table($viewName);
        $this->assertTrue($view->exists(), 'Cross-dataset VIEW should exist in dataset B');

        $rows = iterator_to_array($bqClient->runQuery($bqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketBDatasetName),
            BigqueryQuote::quoteSingleIdentifier($viewName),
        ))));
        $this->assertCount(2, $rows);

        // Verify dataset A's access list contains the authorized view entry
        $credentialsArr = CredentialsHelper::getCredentialsArray($this->projectCredentials);
        $projectId = $credentialsArr['project_id'];

        $sourceDataset = $bqClient->dataset($this->bucketDatasetName);
        $info = $sourceDataset->reload();
        /** @var list<array<string, mixed>> $accessList */
        $accessList = $info['access'] ?? [];

        $expectedEntry = [
            'projectId' => $projectId,
            'datasetId' => $bucketBDatasetName,
            'tableId' => $viewName,
        ];

        $found = false;
        foreach ($accessList as $entry) {
            if (isset($entry['view']) && $entry['view'] === $expectedEntry) {
                $found = true;
                break;
            }
        }
        $this->assertTrue(
            $found,
            'Source dataset access list should contain authorized view entry for cross-dataset VIEW',
        );

        // Verify idempotency: re-running handler should not duplicate the grant
        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $info = $sourceDataset->reload();
        /** @var list<array<string, mixed>> $accessListAfterRerun */
        $accessListAfterRerun = $info['access'] ?? [];
        $grantCount = 0;
        foreach ($accessListAfterRerun as $entry) {
            if (isset($entry['view']) && $entry['view'] === $expectedEntry) {
                $grantCount++;
            }
        }
        $this->assertSame(1, $grantCount, 'Authorized view entry should not be duplicated on re-run');

        // Cleanup: drop VIEW and dataset B
        $dropHandler = new DropTableHandler($this->clientManager);
        $dropHandler->setInternalLogger($this->log);
        $dropHandler(
            $this->projectCredentials,
            (new DropTableCommand())
                ->setPath($path)
                ->setTableName($viewName),
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        try {
            $bbDataset = $bqClient->dataset($bucketBDatasetName);
            if ($bbDataset->exists()) {
                $bbDataset->delete(['deleteContents' => true]);
            }
        } catch (Throwable) {
            // ignore
        }
    }
}
