<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Workspace;

use Google\Cloud\Core\Exception\ServiceException;
use Google\Protobuf\Any;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\CreateWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Drop\DropWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Command\Workspace\DirectGrantTable;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use PHPUnit\Framework\Attributes\Group;
use Throwable;

#[Group('sync')]
class DirectGrantWorkspaceTest extends BaseCase
{
    protected GenericBackendCredentials $projectCredentials;

    protected CreateProjectResponse $projectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];
        $this->projectResponse = $this->projects[0][1];
    }

    public function testCreateWorkspaceWithDirectGrantTables(): void
    {
        // 1. Create a bucket with a table that we will grant access to
        $bucketResponse = $this->createTestBucket($this->projectCredentials);
        $bucketDatasetName = $bucketResponse->getCreateBucketObjectName();

        $grantedTableName = 'granted_table';
        $grantedTable2Name = 'granted_table_2';
        $nonGrantedTableName = 'non_granted_table';

        $tableStructure = [
            'columns' => [
                'id' => [
                    'type' => Bigquery::TYPE_INT64,
                    'nullable' => false,
                    'length' => '',
                ],
                'name' => [
                    'type' => Bigquery::TYPE_STRING,
                    'nullable' => true,
                    'length' => '',
                ],
            ],
            'primaryKeysNames' => [],
        ];

        // Create all tables in bucket using project credentials
        $this->createTable($this->projectCredentials, $bucketDatasetName, $grantedTableName, $tableStructure);
        $this->createTable($this->projectCredentials, $bucketDatasetName, $grantedTable2Name, $tableStructure);
        $this->createTable($this->projectCredentials, $bucketDatasetName, $nonGrantedTableName, $tableStructure);

        // Insert some initial data into all tables
        $bqProjectClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        $bqProjectClient->runQuery($bqProjectClient->query(sprintf(
            'INSERT INTO %s.%s (`id`, `name`) VALUES (1, \'initial\')',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($grantedTableName),
        )));
        $bqProjectClient->runQuery($bqProjectClient->query(sprintf(
            'INSERT INTO %s.%s (`id`, `name`) VALUES (1, \'initial\')',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($grantedTable2Name),
        )));
        $bqProjectClient->runQuery($bqProjectClient->query(sprintf(
            'INSERT INTO %s.%s (`id`, `name`) VALUES (1, \'initial\')',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($nonGrantedTableName),
        )));

        // 2. Create workspace WITH direct grant on granted_table
        $workspaceId = 'WS' . substr($this->getTestHash(), -7) . self::getRand();
        // Cleanup any pre-existing workspace dataset
        $nameGenerator = new NameGenerator($this->getStackPrefix());
        $wsDatasetName = $nameGenerator->createWorkspaceObjectNameForWorkspaceId($workspaceId);
        $bqCleanupClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);
        try {
            $bqCleanupClient->dataset($wsDatasetName)->delete(['deleteContents' => true]);
        } catch (Throwable) {
            // ignore if not exists
        }

        $handler = new CreateWorkspaceHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new CreateWorkspaceCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setWorkspaceId($workspaceId)
            ->setProjectReadOnlyRoleName($this->projectResponse->getProjectReadOnlyRoleName());

        // Add direct grant tables
        $command->getDirectGrantTables()[] = (new DirectGrantTable())
            ->setPath(ProtobufHelper::arrayToRepeatedString([$bucketDatasetName]))
            ->setTableName($grantedTableName);
        $command->getDirectGrantTables()[] = (new DirectGrantTable())
            ->setPath(ProtobufHelper::arrayToRepeatedString([$bucketDatasetName]))
            ->setTableName($grantedTable2Name);

        $meta = new Any();
        $meta->pack(new CreateWorkspaceCommand\CreateWorkspaceBigqueryMeta());
        $command->setMeta($meta);

        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        // Build workspace credentials
        $wsMeta = new Any();
        $wsMeta->pack(
            (new GenericBackendCredentials\BigQueryCredentialsMeta())
                ->setRegion(self::DEFAULT_LOCATION),
        );
        $wsCredentials = (new GenericBackendCredentials())
            ->setHost($this->projectCredentials->getHost())
            ->setPrincipal($response->getWorkspaceUserName())
            ->setSecret($response->getWorkspacePassword())
            ->setPort($this->projectCredentials->getPort());
        $wsCredentials->setMeta($wsMeta);

        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $wsCredentials);

        // 3. Verify workspace SA CAN read from both tables (via dataViewer)
        $readResult = $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($grantedTableName),
        )));
        $this->assertCount(1, $readResult);

        $readResult = $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($nonGrantedTableName),
        )));
        $this->assertCount(1, $readResult);

        // 4. Verify workspace SA CAN write to granted table (INSERT)
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'INSERT INTO %s.%s (`id`, `name`) VALUES (2, \'from_workspace\')',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($grantedTableName),
        )));

        $readResult = $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'SELECT * FROM %s.%s ORDER BY `id`',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($grantedTableName),
        )));
        $this->assertCount(2, $readResult);

        // 5. Verify workspace SA CAN UPDATE granted table
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'UPDATE %s.%s SET `name` = \'updated\' WHERE `id` = 1',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($grantedTableName),
        )));

        // 6. Verify workspace SA CAN DELETE from granted table
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'DELETE FROM %s.%s WHERE `id` = 2',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($grantedTableName),
        )));

        $readResult = $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($grantedTableName),
        )));
        $this->assertCount(1, $readResult);

        // 6b. Verify workspace SA CAN also write to granted_table_2 (INSERT + SELECT)
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'INSERT INTO %s.%s (`id`, `name`) VALUES (2, \'from_workspace\')',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($grantedTable2Name),
        )));

        $readResult = $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'SELECT * FROM %s.%s ORDER BY `id`',
            BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            BigqueryQuote::quoteSingleIdentifier($grantedTable2Name),
        )));
        $this->assertCount(2, $readResult);

        // 7. Verify workspace SA CANNOT write to non-granted table
        try {
            $wsBqClient->runQuery($wsBqClient->query(sprintf(
                'INSERT INTO %s.%s (`id`, `name`) VALUES (99, \'should_fail\')',
                BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
                BigqueryQuote::quoteSingleIdentifier($nonGrantedTableName),
            )));
            $this->fail('Insert to non-granted table should fail with 403');
        } catch (ServiceException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertStringContainsString('Access Denied', $e->getMessage());
        }

        // 8. Verify workspace SA CANNOT create new tables in bucket dataset
        try {
            $wsBqClient->runQuery($wsBqClient->query(sprintf(
                'CREATE TABLE %s.`unauthorized_table` (`id` INT64)',
                BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
            )));
            $this->fail('Creating table in bucket dataset should fail');
        } catch (ServiceException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertStringContainsString('bigquery.tables.create', $e->getMessage());
        }

        // 9. Verify workspace SA CANNOT delete NON-granted table from bucket dataset
        try {
            $wsBqClient->runQuery($wsBqClient->query(sprintf(
                'DROP TABLE %s.%s',
                BigqueryQuote::quoteSingleIdentifier($bucketDatasetName),
                BigqueryQuote::quoteSingleIdentifier($nonGrantedTableName),
            )));
            $this->fail('Dropping non-granted table in bucket dataset should fail');
        } catch (ServiceException $e) {
            $this->assertSame(403, $e->getCode());
        }

        // 10. Cleanup - drop workspace with directGrantTables for IAM revoke
        $dropHandler = new DropWorkspaceHandler($this->clientManager);
        $dropHandler->setInternalLogger($this->log);
        $dropCommand = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($response->getWorkspaceUserName())
            ->setWorkspaceRoleName($response->getWorkspaceRoleName())
            ->setWorkspaceObjectName($response->getWorkspaceObjectName());
        $dropCommand->setIsCascade(true);
        $dropCommand->getDirectGrantTables()[] = (new DirectGrantTable())
            ->setPath(ProtobufHelper::arrayToRepeatedString([$bucketDatasetName]))
            ->setTableName($grantedTableName);
        $dropCommand->getDirectGrantTables()[] = (new DirectGrantTable())
            ->setPath(ProtobufHelper::arrayToRepeatedString([$bucketDatasetName]))
            ->setTableName($grantedTable2Name);
        $dropHandler(
            $this->projectCredentials,
            $dropCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // 11. Verify table-level IAM bindings were removed (no ghost bindings)
        /** @var array{client_email: string} $wsKeyData */
        $wsKeyData = json_decode($response->getWorkspaceUserName(), true, 512, JSON_THROW_ON_ERROR);
        $wsServiceAccountEmail = $wsKeyData['client_email'];
        $saMember = 'serviceAccount:' . $wsServiceAccountEmail;

        foreach ([$grantedTableName, $grantedTable2Name] as $tableName) {
            $table = $bqProjectClient->dataset($bucketDatasetName)->table($tableName);
            /** @var array{bindings?: array<int, array{role: string, members?: array<string>}>} $policy */
            $policy = $table->iam()->policy();
            foreach ($policy['bindings'] ?? [] as $binding) {
                foreach ($binding['members'] ?? [] as $member) {
                    $this->assertNotSame(
                        $saMember,
                        $member,
                        sprintf(
                            'Table IAM binding for workspace SA should have been revoked on %s',
                            $tableName,
                        ),
                    );
                }
            }
        }
    }

    public function testCreateWorkspaceWithNoDirectGrantTablesWorksSameAsWithout(): void
    {
        // Create workspace WITHOUT any direct grant tables (empty list - default behavior)
        [
            $wsCredentials,
            $wsResponse,
        ] = $this->createTestWorkspace($this->projectCredentials, $this->projectResponse);

        $this->assertInstanceOf(GenericBackendCredentials::class, $wsCredentials);
        $this->assertInstanceOf(CreateWorkspaceResponse::class, $wsResponse);

        // Workspace should work normally - can create tables in own dataset
        $wsBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $wsCredentials);
        $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'CREATE TABLE %s.`test_table` (`id` INT64)',
            BigqueryQuote::quoteSingleIdentifier($wsResponse->getWorkspaceObjectName()),
        )));

        // Verify the table was created
        $result = $wsBqClient->runQuery($wsBqClient->query(sprintf(
            'SELECT * FROM %s.`test_table`',
            BigqueryQuote::quoteSingleIdentifier($wsResponse->getWorkspaceObjectName()),
        )));
        $this->assertCount(0, $result);

        // Cleanup
        $dropHandler = new DropWorkspaceHandler($this->clientManager);
        $dropHandler->setInternalLogger($this->log);
        $dropCommand = (new DropWorkspaceCommand())
            ->setWorkspaceUserName($wsResponse->getWorkspaceUserName())
            ->setWorkspaceRoleName($wsResponse->getWorkspaceRoleName())
            ->setWorkspaceObjectName($wsResponse->getWorkspaceObjectName());
        $dropCommand->setIsCascade(true);
        $dropHandler(
            $this->projectCredentials,
            $dropCommand,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }
}
