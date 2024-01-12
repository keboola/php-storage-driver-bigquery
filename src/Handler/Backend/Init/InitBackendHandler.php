<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Backend\Init;

use Google\ApiCore\ApiException;
use Google\ApiCore\ValidationException;
use Google\Cloud\Iam\V1\Binding;
use Google\Cloud\Iam\V1\Policy;
use Google\Cloud\Iam\V1\TestIamPermissionsResponse;
use Google\Protobuf\Internal\Message;
use JsonException;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\Command\Backend\InitBackendCommand;
use Keboola\StorageDriver\Command\Backend\InitBackendResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

final class InitBackendHandler extends BaseHandler
{
    private const EXPECTED_PROJECT_ROLES = ['roles/owner', 'roles/storage.objectAdmin'];
    private const EXPECTED_BILLING_ROLES = ['roles/billing.user'];
    private const EXPECTED_FOLDER_PERMISSIONS = [
        // roles/resourcemanager.projectCreator
        'resourcemanager.projects.create',
//        // roles/browser
        'resourcemanager.folders.get',
        'resourcemanager.folders.list',
        'resourcemanager.projects.get',
        'resourcemanager.projects.getIamPolicy',
        'resourcemanager.projects.list',
    ];

    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @throws JsonException
     * @throws Exception
     * @throws ValidationException
     * @throws ApiException
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof InitBackendCommand);
        assert($runtimeOptions->getMeta() === null);

        $meta = $credentials->getMeta();
        if ($meta !== null) {
            // override root user and use other database as root
            $meta = $meta->unpack();
            assert($meta instanceof GenericBackendCredentials\BigQueryCredentialsMeta);
            $folderId = $meta->getFolderId();
        } else {
            throw new Exception('BigQueryCredentialsMeta is required.');
        }

        /** @var InitExceptionDTO[] $exceptions */
        $exceptions = [];

        $foldersClient = $this->clientManager->getFoldersClient($credentials);
        $folderName = $foldersClient::folderName($folderId);
        try {
            $foldersClient->getFolder($folderName);
        } catch (ApiException $e) {
            $exceptions[] = new InitExceptionDTO(
                sprintf(
                    'Cannot fetch folder "%s" detail.',
                    $folderName,
                ),
                $e->getReason(),
            );
        }

        // check folder and permissions
        // we can't use getIamPolicy we are missing resourcemanager.folders.getIamPolicy
        $folderPermissions = null;
        try {
            $folderPermissions = $foldersClient->testIamPermissions(
                $folderName,
                self::EXPECTED_FOLDER_PERMISSIONS,
            );
        } catch (ApiException $e) {
            $exceptions[] = new InitExceptionDTO(
                sprintf(
                    'Cannot get permissions for folder "%s" expected permission "%s" was not probably assigned.',
                    $folderName,
                    implode(', ', self::EXPECTED_FOLDER_PERMISSIONS),
                ),
                $e->getReason(),
            );
        } finally {
            $foldersClient->close();
        }
        $this->assertPermissions($exceptions, self::EXPECTED_FOLDER_PERMISSIONS, $folderPermissions);

        // check root project roles
        $projectsClient = $this->clientManager->getProjectClient($credentials);
        /** @var array<string, string> $principal */
        $principal = (array) json_decode($credentials->getPrincipal(), true, 512, JSON_THROW_ON_ERROR);
        $projectNameFormatted = $projectsClient::projectName($principal['project_id']);

        $policies = null;
        try {
            $policies = $projectsClient->getIamPolicy($projectNameFormatted);
        } catch (ApiException $e) {
            $exceptions[] = new InitExceptionDTO(
                sprintf(
                    'Cannot get roles for project "%s" expected roles "%s" was not probably assigned.',
                    $projectNameFormatted,
                    implode(', ', self::EXPECTED_PROJECT_ROLES),
                ),
                $e->getReason(),
            );
        } finally {
            $projectsClient->close();
        }
        $roles = $this->getRolesFromPolicy($policies);
        $this->assertRoles($exceptions, self::EXPECTED_PROJECT_ROLES, $roles);

        // check billing account roles
        $billingClient = $this->clientManager->getBillingClient($credentials);
        $billingInfo = null;
        try {
            $billingInfo = $billingClient->getProjectBillingInfo($projectNameFormatted);
        } catch (ApiException $e) {
            $exceptions[] = new InitExceptionDTO(
                sprintf(
                    'Cannot fetch project "%s" billing info.',
                    $projectNameFormatted,
                ),
                $e->getReason(),
            );
        }
        if ($billingInfo !== null) {
            $mainBillingAccount = $billingInfo->getBillingAccountName();
            try {
                $policies = $billingClient->getIamPolicy($mainBillingAccount);
            } catch (ApiException $e) {
                $exceptions[] = new InitExceptionDTO(
                    sprintf(
                        'Cannot get roles for billing account "%s" expected roles "%s" was not probably assigned.',
                        $mainBillingAccount,
                        implode(', ', self::EXPECTED_BILLING_ROLES),
                    ),
                    $e->getReason(),
                );
            } finally {
                $billingClient->close();
            }
            $roles = $this->getRolesFromPolicy($policies);
            $this->assertRoles($exceptions, self::EXPECTED_BILLING_ROLES, $roles);
        }

        InitBackendFailedException::handleExceptions($exceptions);

        return new InitBackendResponse();
    }

    /**
     * @param string[] $expectedPermissions
     * @param InitExceptionDTO[] $exceptions
     */
    private function assertPermissions(
        array &$exceptions,
        array $expectedPermissions,
        TestIamPermissionsResponse|null $billingPermissions,
    ): void {
        if ($billingPermissions === null) {
            $missingPermissions = $expectedPermissions;
        } else {
            $missingPermissions = array_diff(
                $expectedPermissions,
                iterator_to_array($billingPermissions->getPermissions()),
            );
        }

        if (count($missingPermissions) !== 0) {
            $exceptions[] = new InitExceptionDTO(
                sprintf(
                    'Missing permissions "%s" for service account.',
                    implode(', ', $missingPermissions),
                ),
                null,
            );
        }
    }

    /**
     * @param string[] $expectedRoles
     * @param string[] $roles
     * @param InitExceptionDTO[] $exceptions
     */
    private function assertRoles(array &$exceptions, array $expectedRoles, array $roles): void
    {
        $missingRoles = array_diff(
            $expectedRoles,
            $roles,
        );

        if (count($missingRoles) !== 0) {
            $exceptions[] = new InitExceptionDTO(
                sprintf(
                    'Missing roles "%s" for service account.',
                    implode(', ', $missingRoles),
                ),
                null,
            );
        }
    }

    /**
     * @return string[]
     */
    private function getRolesFromPolicy(Policy|null $policies): array
    {
        if ($policies === null) {
            return [];
        }
        /** @var Binding[] $bindings */
        $bindings = iterator_to_array($policies->getBindings());
        return array_map(
            fn(Binding $b) => $b->getRole(),
            $bindings,
        );
    }
}
