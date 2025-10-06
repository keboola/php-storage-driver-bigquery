<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Backend\Init\InitBackendHandler;
use Keboola\StorageDriver\BigQuery\Handler\Backend\Remove\RemoveBackendHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\CreateBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\GrantBucketAccessToReadOnlyRoleHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop\DropBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Drop\RevokeBucketAccessFromReadOnlyRoleHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnLink\UnLinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnShare\UnShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\ExecuteQuery\ExecuteQueryHandler;
use Keboola\StorageDriver\BigQuery\Handler\Info\ObjectInfoHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Create\CreateProjectHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Drop\DropProjectHandler;
use Keboola\StorageDriver\BigQuery\Handler\Project\Update\UpdateProjectHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\AddColumnHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\AddPrimaryKeyHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\AlterColumnHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\DeleteTableRowsHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\DropColumnHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Alter\DropPrimaryKeyHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableFromTimeTravelHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Export\ExportTableToFileHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromFileHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Import\ImportTableFromTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Preview\PreviewTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Profile\ProfileTableHandler;
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
use Keboola\StorageDriver\Command\ExecuteQuery\ExecuteQueryCommand;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Project\CreateDevBranchCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\DropDevBranchCommand;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Command\Project\UpdateProjectCommand;
use Keboola\StorageDriver\Command\Table\AddColumnCommand;
use Keboola\StorageDriver\Command\Table\AddPrimaryKeyCommand;
use Keboola\StorageDriver\Command\Table\AlterColumnCommand;
use Keboola\StorageDriver\Command\Table\CreateProfileTableCommand;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\CreateTableFromTimeTravelCommand;
use Keboola\StorageDriver\Command\Table\DeleteTableRowsCommand;
use Keboola\StorageDriver\Command\Table\DropColumnCommand;
use Keboola\StorageDriver\Command\Table\DropPrimaryKeyCommand;
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
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\CommandNotSupportedException;
use Psr\Log\LoggerInterface;

final class HandlerFactory
{
    public static function create(
        Message $command,
        GCPClientManager $manager,
        LoggerInterface $internalLogger,
    ): DriverCommandHandlerInterface {
        $handler = match ($command::class) {
            InitBackendCommand::class => new InitBackendHandler($manager),
            RemoveBackendCommand::class => new RemoveBackendHandler(),
            CreateProjectCommand::class => new CreateProjectHandler($manager),
            UpdateProjectCommand::class => new UpdateProjectHandler($manager),
            DropProjectCommand::class => new DropProjectHandler($manager),
            CreateBucketCommand::class => new CreateBucketHandler($manager),
            DropBucketCommand::class => new DropBucketHandler($manager),
            ShareBucketCommand::class => new ShareBucketHandler($manager),
            UnshareBucketCommand::class => new UnShareBucketHandler($manager),
            LinkBucketCommand::class => new LinkBucketHandler($manager),
            UnlinkBucketCommand::class => new UnLinkBucketHandler($manager),
            CreateTableCommand::class => new CreateTableHandler($manager),
            DropTableCommand::class => new DropTableHandler($manager),
            AddColumnCommand::class => new AddColumnHandler($manager),
            AlterColumnCommand::class => new AlterColumnHandler($manager),
            DropColumnCommand::class => new DropColumnHandler($manager),
            TableImportFromFileCommand::class => new ImportTableFromFileHandler($manager),
            TableImportFromTableCommand::class => new ImportTableFromTableHandler($manager),
            PreviewTableCommand::class => new PreviewTableHandler($manager),
            TableExportToFileCommand::class => new ExportTableToFileHandler($manager),
            CreateWorkspaceCommand::class => new CreateWorkspaceHandler($manager),
            DropWorkspaceCommand::class => new DropWorkspaceHandler($manager),
            ResetWorkspacePasswordCommand::class => new ResetWorkspacePasswordHandler($manager),
            ClearWorkspaceCommand::class => new ClearWorkspaceHandler($manager),
            DropWorkspaceObjectCommand::class => new DropWorkspaceObjectHandler($manager),
            ObjectInfoCommand::class => new ObjectInfoHandler($manager),
            DeleteTableRowsCommand::class => new DeleteTableRowsHandler($manager),
            CreateTableFromTimeTravelCommand::class => new CreateTableFromTimeTravelHandler($manager),
            GrantBucketAccessToReadOnlyRoleCommand::class => new GrantBucketAccessToReadOnlyRoleHandler($manager),
            RevokeBucketAccessFromReadOnlyRoleCommand::class => new RevokeBucketAccessFromReadOnlyRoleHandler($manager),
            AddPrimaryKeyCommand::class => new AddPrimaryKeyHandler($manager),
            DropPrimaryKeyCommand::class => new DropPrimaryKeyHandler($manager),
            CreateProfileTableCommand::class => new ProfileTableHandler($manager),
            ExecuteQueryCommand::class => new ExecuteQueryHandler($manager),
            CreateDevBranchCommand::class,
            DropDevBranchCommand::class => new EmptyHandler(),
            default => throw new CommandNotSupportedException($command::class),
        };

        $handler->setInternalLogger($internalLogger);

        return $handler;
    }
}
