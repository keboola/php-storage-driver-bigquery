<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Project\Drop;

class PolicyFilter
{
    public static function removeServiceAccFromBucketPolicy(array $policy, string $serviceAccountEmail): array
    {
        foreach ($policy['bindings'] as $bindingKey => $binding) {
            if ($binding['role'] === 'roles/storage.objectAdmin') {
                $key = array_search('serviceAccount:' . $serviceAccountEmail, $binding['members'], false);
                if ($key === false) {
                    continue;
                }
                unset($policy['bindings'][$bindingKey]['members'][$key]);
            }
        }

        self::recalculateBindingKeys($policy);
        return $policy;
    }

    // The keys for members in binding have to be recalculated,
    // because if there is an error in the queue number GCP will delete all permissions
    private static function recalculateBindingKeys(&$array): void
    {
        if (isset($array['bindings']) && is_array($array['bindings'])) {
            foreach ($array['bindings'] as &$binding) {
                if (isset($binding['members']) && is_array($binding['members'])) {
                    $binding['members'] = array_values($binding['members']);
                }
            }
        }
    }
}
