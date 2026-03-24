<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Bucket\Link;

use Google\ApiCore\ApiException;
use Google\Cloud\Iam\V1\Binding;
use Google\Protobuf\Internal\Message;
use Google\Rpc\Code;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\Command\Bucket\RevokeExternalBucketSubscriberCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

/**
 * Revokes roles/analyticshub.subscriber from a service account on an Analytics Hub listing.
 * Must be called with KBC1's PROJECT credentials (which have listingAdmin on the listing).
 */
final class RevokeExternalBucketSubscriberHandler extends BaseHandler
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
        assert($command instanceof RevokeExternalBucketSubscriberCommand);

        $listing = $command->getListingName();
        $subscriberEmail = $command->getSubscriberServiceAccountEmail();

        assert($listing !== '', 'RevokeExternalBucketSubscriberCommand.listingName must be filled in');
        assert(
            $subscriberEmail !== '',
            'RevokeExternalBucketSubscriberCommand.subscriberServiceAccountEmail must be filled in',
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

                /** @var Binding[] $newBindings */
                $newBindings = [];
                $changed = false;

                /** @var Binding $binding */
                foreach ($existingBindings as $binding) {
                    if ($binding->getRole() === $subscriberRole) {
                        $currentMembers = iterator_to_array($binding->getMembers());
                        /** @var list<string> $remainingMembers */
                        $remainingMembers = array_values(array_filter(
                            $currentMembers,
                            fn(mixed $m) => $m !== $subscriberMember,
                        ));
                        if (count($remainingMembers) !== count($currentMembers)) {
                            $changed = true;
                        }
                        if (count($remainingMembers) > 0) {
                            // Mutate existing binding to preserve any additional fields (e.g., conditions)
                            $binding->setMembers($remainingMembers);
                            $newBindings[] = $binding;
                        }
                        // if empty, drop the binding entirely by not adding it to $newBindings
                    } else {
                        $newBindings[] = $binding;
                    }
                }

                if (!$changed) {
                    return; // already not a subscriber, nothing to do
                }

                // Update the current policy in place to preserve other fields (e.g. version, audit configs)
                $currentPolicy->setBindings($newBindings);
                $analyticHubClient->setIamPolicy($listing, $currentPolicy);
            });
        // @phpstan-ignore-next-line - ApiException is thrown inside the RetryProxy closure
        } catch (ApiException $e) {
            if ($e->getCode() === Code::PERMISSION_DENIED) {
                throw RevokeExternalBucketSubscriberPermissionDeniedException::fromApiException($e, $listing);
            }
            throw $e;
        }

        return null;
    }
}
