<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Link;

use Google\ApiCore\ApiException;
use Google\Cloud\Iam\V1\Binding;
use Google\Cloud\Iam\V1\Policy;
use Google\Protobuf\Internal\Message;
use Google\Rpc\Code;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Bucket\GrantExternalBucketSubscriberCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

/**
 * Grants roles/analyticshub.subscriber to a service account on an Analytics Hub listing.
 * Must be called with KBC1's PROJECT credentials (which have listingAdmin on the listing).
 */
final class GrantExternalBucketSubscriberHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials KBC1's PROJECT credentials
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof GrantExternalBucketSubscriberCommand);

        $listing = $command->getListingName();
        $subscriberEmail = $command->getSubscriberServiceAccountEmail();

        assert($listing !== '', 'GrantExternalBucketSubscriberCommand.listingName must be filled in');
        assert(
            $subscriberEmail !== '',
            'GrantExternalBucketSubscriberCommand.subscriberServiceAccountEmail must be filled in',
        );

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($credentials);

        $subscriberMember = 'serviceAccount:' . $subscriberEmail;
        $subscriberRole = 'roles/analyticshub.subscriber';

        $retryPolicy = new CallableRetryPolicy(function (Throwable $e) {
            if (in_array($e->getCode(), GCPClientManager::ERROR_CODES_FOR_RETRY_IAM)) {
                return true;
            }
            return false;
        }, 20);
        $proxy = new RetryProxy($retryPolicy, new ExponentialRandomBackOffPolicy());
        try {
            $proxy->call(function () use ($analyticHubClient, $listing, $subscriberRole, $subscriberMember): void {
                $currentPolicy = $analyticHubClient->getIamPolicy($listing);
                $existingBindings = $currentPolicy->getBindings();

                /** @var Binding $binding */
                foreach ($existingBindings as $binding) {
                    if ($binding->getRole() === $subscriberRole) {
                        foreach ($binding->getMembers() as $member) {
                            if ($member === $subscriberMember) {
                                return; // already granted
                            }
                        }
                    }
                }

                /** @var Binding[] $newBindings */
                $newBindings = iterator_to_array($existingBindings);
                $newBindings[] = new Binding([
                    'role' => $subscriberRole,
                    'members' => [$subscriberMember],
                ]);
                $newPolicy = new Policy();
                $newPolicy->setBindings($newBindings);
                $newPolicy->setEtag($currentPolicy->getEtag());
                $analyticHubClient->setIamPolicy($listing, $newPolicy);
            });
        // @phpstan-ignore-next-line - ApiException is thrown inside the RetryProxy closure
        } catch (ApiException $e) {
            if ($e->getCode() === Code::PERMISSION_DENIED) {
                throw GrantExternalBucketSubscriberPermissionDeniedException::fromApiException($e, $listing);
            }
            throw $e;
        }

        return null;
    }
}
