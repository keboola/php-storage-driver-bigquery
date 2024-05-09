<?php

if (!file_exists('./principal_key.json')) {
    echo 'Error: Failed to read principal_key.json. Exiting';
    exit();
}
if (!file_exists('./big_query_key.json')) {
    echo 'Error: Failed to read big_query_key.json. Exiting';
    exit();
}

$principalKey = json_decode(file_get_contents('./principal_key.json'), true);
$bqKey = json_decode(file_get_contents('./big_query_key.json'), true);

echo sprintf("BQ_SECRET=\"%s\"\n", str_replace("\n", "\\n", $principalKey['private_key']));
unset($principalKey['private_key']);
echo sprintf("BQ_PRINCIPAL=%s\n", json_encode($principalKey));
echo sprintf("BQ_KEY_FILE=%s\n", json_encode($bqKey));
