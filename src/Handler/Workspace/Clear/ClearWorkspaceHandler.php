<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Workspace\Clear;

use Google\Cloud\BigQuery\Table;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Workspace\ClearWorkspaceCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Throwable;

final class ClearWorkspaceHandler extends BaseHandler
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
     * @param ClearWorkspaceCommand $command
     */
    public function __invoke(
        Message $credentials, // project credentials
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof ClearWorkspaceCommand);

        assert($runtimeOptions->getMeta() === null);

        // validate
        assert($command->getWorkspaceObjectName() !== '', 'ClearWorkspaceCommand.workspaceObjectName is required');

        /** @var array<string, string> $queryTags */
        $queryTags = iterator_to_array($runtimeOptions->getQueryTags());

        $bqClient = $this->clientManager->getBigQueryClient(
            $runtimeOptions->getRunId(),
            $credentials,
            $queryTags,
        );

        $dataset = $bqClient->dataset($command->getWorkspaceObjectName());
        try {
            // SUPPORT-15365: Use a separate throwaway iterator for the dataset existence check.
            // Calling current() on an iterator before foreach corrupts pagination state —
            // rewind() does not reset the pageToken, causing foreach to skip the first page.
            $dataset->tables()->current();
        } catch (Throwable $e) {
            if (!$command->getIgnoreErrors()) {
                throw $e;
            }
            return null;
        }

        $preserveTables = ProtobufHelper::repeatedStringToArray($command->getObjectsToPreserve());

        /** @var Table $table */
        foreach ($dataset->tables() as $table) {
            if (in_array($table->id(), $preserveTables, true)) {
                continue;
            }
            try {
                $table->delete();
            } catch (Throwable $e) {
                if (!$command->getIgnoreErrors()) {
                    throw $e;
                }
            }
        }

        return null;
    }
}
