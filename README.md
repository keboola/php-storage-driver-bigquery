# Keboola Storage Driver Big Query

Keboola high level storage backend driver for Big Query

## Install dependencies.

```bash
docker compose run --rm dev composer install
```

## Setup Big Query

Install [Google Cloud client](https://cloud.google.com/sdk/docs/install-sdk) (via [Brew](https://formulae.brew.sh/cask/google-cloud-sdk#default)), initialize it
and log in to [generate default credentials](https://cloud.google.com/docs/authentication/application-default-credentials#personal).

To prepare the backend use [Terraform template](./bq-storage-backend-init.tf).
Create a sub folder in the **KBC Team Dev** (id: [431160969986](https://console.cloud.google.com/cloud-resource-manager?folder=431160969986)) folder and fill the folder into the terraform command.
1. get missing pieces (organization_id and billing_id) from [Connection repository](https://github.com/keboola/connection/blob/master/docs/DOCKER.md#bigquery).
2. (optional) move `bq-storage-backend-init.tf` out of project directory so new files would be out of git
3. Run `terraform init` 
4. Run `terraform apply -var folder_id=[folder_id] -var billing_account_id=[billing_id] -var backend_prefix=<your prefix, eg. kbc-js> -var file_storage_backend_region=<desired region>` (e.g. us-central1 region)
   1. Optionally set `terraform.tfvars` file with variables there is predefined `terraform.tfvars.dist` file 
5. New key files was created `principal_key.json` and `big_query_key.json`
6. open `principal_key.json` set content of `private_key` as variable `BQ_SECRET` and remove (the whole entry) it from json file
   1. note: simply cut&paste it whole even with the quotes and new lines -> your .env will be like `BQ_SECRET="-----BEGIN PRIVATE KEY-----XXXXZQ==\n-----END PRIVATE KEY-----\n"`
7. remove line breaks from the rest of key file (without `private_key` entry) and set this string as variable `BQ_PRINCIPAL` to `.env` 
   1. You can convert the key to string with `awk -v RS= '{$1=$1}1' principal_key.json`
8. remove line breaks from the `big_query_key.json` key file and set this string as variable `BQ_KEY_FILE` to `.env`
   1. You can convert the key to string with `awk -v RS= '{$1=$1}1' big_query_key.json`

At the end, your `.env` file should look like...
```bash
# the id is printed by terraform at the end and it is just the numbers after `folders/`

BQ_PRINCIPAL=<the content of the principal_key.json key file as single line without private_key entry>
BQ_SECRET=<private_key from principal_key.json key file (taken from BQ_PRINCIPAL)>

BQ_FOLDER_ID=<TF output file_storage_bucket_id : the id of the created folder, just the number, without /folders prefix>
BQ_BUCKET_NAME=<TF output file_storage_bucket_id : bucket id created in main project>

# choose different BQ_STACK_PREFIX than you Terraform prefix otherwise project created by Terraform will be deleted . e.g. local :)
BQ_STACK_PREFIX=local

BQ_KEY_FILE=<big_query_key.json file owned by main service acc>
```

All done. Now you can try `composer loadGcs` script and run tests.
```bash
docker compose run --rm dev composer loadGcs
```

## Build docker images

```bash
docker compose build
```

## Xdebug

To run with xdebug use `dev-xdebug` container instead of `dev`

## Tests

Run tests with following command.

```bash
# This will run all tests
docker compose run --rm dev composer tests
# This will run all tests in parallel
docker compose run --rm dev composer paratest
# This will run import tests in parallel
docker compose run --rm dev composer paratest-import
# This will run export tests in parallel
docker compose run --rm dev composer paratest-export
# This will run all tests in parallel excluding import and export
docker compose run --rm dev composer paratest-other
```

To disable retry copy `phpunit-retry.xml.dist`
```bash
cp phpunit-retry.xml.dist phpunit-retry.xml
```

## Code quality check

```bash
#run all bellow but not tests
docker compose run --rm dev composer check

#phplint
docker compose run --rm dev composer phplint

#phpcs
docker compose run --rm dev composer phpcs

#phpcbf
docker compose run --rm dev composer phpcbf

#phpstan
docker compose run --rm dev composer phpstan
```

## Full CI workflow

This command will run all checks and run tests
```bash
docker compose run --rm dev composer ci
```

## Using

Project ID: A globally unique identifier for your project. This lib creating project id as combinations of `stackPrefix` and `projectId` from `CreateProjectCommand`

A project ID is a unique string used to differentiate your project from all others in Google Cloud. 
You can use the Google Cloud console to generate a project ID, or you can choose your own. You can only modify the project ID when you're creating the project.

Project ID requirements:
- Must be 6 to 30 characters in length.
- Can only contain lowercase letters, numbers, and hyphens.
- Must start with a letter.
- Cannot end with a hyphen.
- Cannot be in use or previously used; this includes deleted projects.
- Cannot contain restricted strings, such as `google` and `ssl`.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
