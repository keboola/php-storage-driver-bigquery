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

        return $policy;
    }
    }
}

