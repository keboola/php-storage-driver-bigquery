<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\Handler\Backend\Init\InitBackendHandler;
use Keboola\StorageDriver\BigQuery\Handler\Backend\Remove\RemoveBackendHandler;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\GrantBucketAccessToReadOnlyRoleHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop\DropBucketHandle;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop\RevokeBucketAccessFromReadOnlyRoleHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnLink\UnLinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnShare\UnShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Info\ObjectInfoHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Drop\DropProjectHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\AddColumnHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\DeleteTableRowsHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\DropColumnHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableFromTimeTravelHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Export\ExportTableToFileHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Preview\PreviewTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Clear\ClearWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Create\CreateWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\Drop\DropWorkspaceHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\DropObject\DropWorkspaceObjectHandler;
use Keboola\StorageDriver\BigQuery\Handler\Workspace\ResetPassword\ResetWorkspacePasswordHandler;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\RemoveBackendCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\DropBucketCommand;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleCommand;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\RevokeBucketAccessFromReadOnlyRoleCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Bucket\UnlinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\UnshareBucketCommand;
use Keboola\StorageDriver\Command\Common\DriverResponse;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Command\Table\AddColumnCommand;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\CreateTableFromTimeTravelCommand;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsCommand;
use Keboola\StorageDriver\Command\Table\DropColumnCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\PreviewTableCommand;
use Keboola\StorageDriver\Command\Table\TableExportToFileCommand;
use Keboola\StorageDriver\Command\Table\TableImportFromFileCommand;
use Keboola\StorageDriver\Command\Table\TableImportFromTableCommand;
use Keboola\StorageDriver\Command\Workspace\ClearWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceObjectCommand;
use Keboola\StorageDriver\Command\Workspace\ResetWorkspacePasswordCommand;
use Keboola\StorageDriver\Contract\Driver\ClientInterface;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\CommandNotSupportedException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class BigQueryDriverClient implements ClientInterface
{
    protected LoggerInterface $internalLogger;

    public function __construct(?LoggerInterface $internalLogger = null)
    {
        if ($internalLogger === null) {
            $this->internalLogger = new NullLogger();
        } else {
            $this->internalLogger = $internalLogger;
        }
    }

    /**
     * @param string[] $features
     */
    public function runCommand(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        $manager = new GCPClientManager($this->internalLogger);
        $handler = $this->getHandler($command, $manager);
        $handler->setInternalLogger($this->internalLogger);

        $handledResponse = $handler(
            $credentials,
            $command,
            $features,
            $runtimeOptions,
        );
        $response = new DriverResponse();
        if ($handledResponse !== null) {
            $any = new Any();
            $any->pack($handledResponse);
            $response->setCommandResponse($any);
        }
        $response->setMessages($handler->getMessages());
        return $response;
    }

    /**
     * @return BaseHandler
     */
    private function getHandler(Message $command, GCPClientManager $manager): DriverCommandHandlerInterface
    {
        switch (true) {
            case $command instanceof InitBackendCommand:
                return new InitBackendHandler($manager);
            case $command instanceof RemoveBackendCommand:
                return new RemoveBackendHandler();
            case $command instanceof CreateProjectCommand:
                return new CreateProjectHandler($manager);
            case $command instanceof DropProjectCommand:
                return new DropProjectHandler($manager);
            case $command instanceof CreateBucketCommand:
                return new CreateBucketHandler($manager);
            case $command instanceof DropBucketCommand:
                return new DropBucketHandle($manager);
            case $command instanceof ShareBucketCommand:
                return new ShareBucketHandler($manager);
            case $command instanceof UnshareBucketCommand:
                return new UnShareBucketHandler($manager);
            case $command instanceof LinkBucketCommand:
                return new LinkBucketHandler($manager);
            case $command instanceof UnlinkBucketCommand:
                return new UnLinkBucketHandler($manager);
            case $command instanceof CreateTableCommand:
                return new CreateTableHandler($manager);
            case $command instanceof DropTableCommand:
                return new DropTableHandler($manager);
            case $command instanceof AddColumnCommand:
                return new AddColumnHandler($manager);
            case $command instanceof DropColumnCommand:
                return new DropColumnHandler($manager);
            case $command instanceof TableImportFromFileCommand:
                return new ImportTableFromFileHandler($manager);
            case $command instanceof TableImportFromTableCommand:
                return new ImportTableFromTableHandler($manager);
            case $command instanceof PreviewTableCommand:
                return new PreviewTableHandler($manager);
            case $command instanceof TableExportToFileCommand:
                return new ExportTableToFileHandler($manager);
            case $command instanceof CreateWorkspaceCommand:
                return new CreateWorkspaceHandler($manager);
            case $command instanceof DropWorkspaceCommand:
                return new DropWorkspaceHandler($manager);
            case $command instanceof ResetWorkspacePasswordCommand:
                return new ResetWorkspacePasswordHandler($manager);
            case $command instanceof ClearWorkspaceCommand:
                return new ClearWorkspaceHandler($manager);
            case $command instanceof DropWorkspaceObjectCommand:
                return new DropWorkspaceObjectHandler($manager);
            case $command instanceof ObjectInfoCommand:
                return new ObjectInfoHandler($manager);
            case $command instanceof DeleteTableRowsCommand:
                return new DeleteTableRowsHandler($manager);
            case $command instanceof CreateTableFromTimeTravelCommand:
                return new CreateTableFromTimeTravelHandler($manager);
            case $command instanceof GrantBucketAccessToReadOnlyRoleCommand:
                return new GrantBucketAccessToReadOnlyRoleHandler($manager);
            case $command instanceof RevokeBucketAccessFromReadOnlyRoleCommand:
                return new RevokeBucketAccessFromReadOnlyRoleHandler($manager);
        }

        throw new CommandNotSupportedException(get_class($command));
    }
}
