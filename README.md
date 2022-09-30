# Keboola Storage Driver Big Query

Keboola high level storage backend driver for Big Query

## Setup Big Query

To prepare the backend you can use [Terraform template](./bq-storage-backend-init.tf).
You must have the `resourcemanager.folders.create` permission for the organization.
```bash
# you can copy it to a folder somewhere and make an init
terraform init

terraform apply -var organization_id=[organization_id]
# and enter name for your backend prefix for example your name, all resources will create with this prefx
```

After terraform apply ends go to the service project in folder created by terraform.

1. go to the newly created service project, the project id are listed at the end of the terraform call. (service_project_id)
2. click on IAM & Admin 
3. on left panel choose Service Accounts
4. click on email of service account(there is only one)
5. on to the top choose Keys and Add Key => Create new key
6. select Key type JSON
7. click on the Create button and the file will automatically download
8. convert key to string `awk -v RS= '{$1=$1}1' <key_file>.json >> .env`
9. set content on last line of .env as variable `GCS_CREDENTIALS`

setup envs:
```bash
# the id is printed by terraform at the end and it is just the numbers after `folders/`

BQ_KEYFILE_JSON=<the content of the downloaded json key file>
BQ_FOLDER_ID=<the id of the created folder>
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

## License

MIT licensed, see [LICENSE](./LICENSE) file.
