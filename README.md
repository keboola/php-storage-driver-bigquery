# Keboola Storage Driver Big Query

Keboola high level storage backend driver for Big Query

## Setup Big Query

Install [Google Cloud client](https://cloud.google.com/sdk/docs/install-sdk) (via [Brew](https://formulae.brew.sh/cask/google-cloud-sdk#default)), initialize it
and log in to [generate default credentials](https://cloud.google.com/docs/authentication/application-default-credentials#personal).

To prepare the backend you can use [Terraform template](./bq-storage-backend-init.tf).
You must have the `resourcemanager.folders.create` permission for the organization.
```bash
# you can copy it to a folder somewhere and make an init
terraform init

terraform apply -var organization_id=[organization_id]
# and enter name for your backend prefix for example your name, all resources will create with this prefix
```

For missing pieces see [Connection repository](https://github.com/keboola/connection/blob/master/DOCKER.md#bigquery).

After terraform apply ends go to the service project in folder created by terraform.

1. go to the newly created service project, the project id are listed at the end of the terraform call. (service_project_id)
2. click on IAM & Admin 
3. on left panel choose Service Accounts
4. click on email of service account(there is only one)
5. on to the top choose Keys and Add Key => Create new key
6. select Key type JSON
7. click on the Create button and the file will automatically download
8. open keyFile.json set content of `private_key` as variable `BQ_SECRET` and remove it from json file
9. convert key to string and save to `.env` file: `awk -v RS= '{$1=$1}1' <key_file>.json >> .env`
10. set content on the last line of `.env` as variable `BQ_PRINCIPAL`

setup envs:
```bash
# the id is printed by terraform at the end and it is just the numbers after `folders/`

BQ_PRINCIPAL=<the content without private_key of the downloaded json key file>
BQ_SECRET=<private_key from downloaded json key file>
BQ_FOLDER_ID=<the id of the created folder>
BQ_STACK_PREFIX=<prefix of stack>
```

## Build docker images

```bash
docker-compose build
```

## Xdebug

To run with xdebug use `dev-xdebug` container instead of `dev`

## Tests

Run tests with following command.

```bash
docker-compose run --rm dev composer tests
```

And than run phpunit
```bash
docker-compose run --rm dev composer phpunit
```

To disable retry copy `phpunit-retry.xml.dist`
```bash
cp phpunit-retry.xml.dist phpunit-retry.xml
```

## Code quality check

```bash
#run all bellow but not tests
docker-compose run --rm dev composer check

#phplint
docker-compose run --rm dev composer phplint

#phpcs
docker-compose run --rm dev composer phpcs

#phpcbf
docker-compose run --rm dev composer phpcbf

#phpstan
docker-compose run --rm dev composer phpstan
```

## Full CI workflow

This command will run all checks and run tests
```bash
docker-compose run --rm dev composer ci
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
