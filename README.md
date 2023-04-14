# Data Engineering Final Project

*Project requirements are available [here][file_requirements]*

## Prerequirements

- [Python 3.X][python_web]
- [terraform][terraform_web]
- [AWS CLI][aws_cli]
  - [AWS profile][aws_profile] credentials with S3 permission (Suggested profile name `de_project`)
- [Docker][docker_web]
- [Snowflake][snowflake_web] account (free trial account is OK)
  - Snowflake CLI credentials

## Phase 1 - Data Ingestion

> On the `Data Ingestion phase`, let's consume and store the API Data on the convenient place that you would like to use (it can be either locally, S3 or even on memory on a data frame, or any other storage location place)

**Note:** *This is a preparation step, **suggested to be loaded only once**, the pipeline is ready to process more than 1 file, however this will take a considerable ammount of additional resources*

This pase consists on 2 steps:

- Scrapping data
  - Use Nike API
  - Generate sales
- Loading the data into s3

### Scrapping

*The following commands are used to run the `scrapper/main.py` file this will create the `data/products` and `data/sales` folders with all the raw data required*

From project root:

```sh
cd scrapper
python3 -m venv scrapper_env
pip3 install -r requirements.txt
python3 main.py
```

> Feel free to use another commands if you already know how to run a py file
> scrapper_env is a suggestion for your virtual environment, however this virtual environment is already excluded in the gitignore file

### Uploading files to S3

> Ensure you have your profile created on `.aws` folder
> The profile name must be `de_project`

If you want to use a different one update the file `terraform/main.tf`:

```tf
provider "aws" {
  profile = "your_profile"
  ...
}

locals {
  aws_profile = "your_profile" # must be the same as aws profile provider
  ...
}
```

*The following commands are used to run the `terraform/main.tf` file this will create the `bucket` and `folders` with all the raw data required*

From project root:

```sh
cd terraform
terraform init
terraform plan -out tfplan
terraform apply
```

*You will be asked to confirm your command, if getting prompted you will need to type `yes` then hit `Enter` to continue

## Phase 1.5 - Datawarehouse structure

- Create the database `DE_PROJECT`
- Execute `sql/ddl.sql` on the Snowflake environment

Note: ***COMPUTE_WH** resource is used during the next phase, ensure this resource exists on the Snowflake environment*

## Phase 2-3 - Data Processing & Ingestion

> On the `Data Processing phase`, use the ingested data from the API and transform accordingly to be ready to be ingested into the Data Warehouse
> On the `Data ingestion into Data Warehouse phase`, lets upload the processed API data into the Data Warehouse

SET Environment variables for AWS and Snowflake

This phase will use docker in order to provision and run `airflow/dag/etl.py` via airflow

*Before getting started ensure docker daemon is running*, then; from project root use the following commands

```sh
cd airflow
sh launcher.sh
# airflow/dag/etl.py
```

## Phase 4 - Querying the Datawarehouse

> On the `Querying the Datawarehouse phase`, let's write the following queries:

1. Query the top 5 sales by product
2. Query the top 5 sales by category agrupation
3. Query the least 5 sales by category agrupation
4. Query the top 5 sales by title and subtitle agrupation
5. Query the top 3 products that has greatest sales by category

## Other deliverables

> - Create the necessary files of your environment and make a walkthrough of the code of your solution on a README
> - For the Datawarehouse, put on a file the DDL statements that were used to create the tables
> - For the Queries of the Datawarehouse, put on a file the Queries statements requested on the `Querying the Datawarehouse phase`

## Considerations

The original requirement suggested the following:

> · The Data Warehouse will be used to calculate the sales of the company, the sales will be calculated as:
> · For all the products that the API ingested, sales will be the sum of the Current Price

However, this was not ideal as only 1 sale per product will be created; instead an additional script was created for the scrapper that can create a wide variety of random sales on the `scrapper/sales_generator.py` file

The original snippet was updated in `scrapper/main.py` and also `scrapper/nikescrapi.py` was cleaned to remove unused imports, unused methods and make private some public methods that were only internally used

### Sales generator constraints

Private properties:
*These properties are static in the class*

```txt
min_qty: Minimum items per ticket (must not be zero)
max_qty: Maximum items per ticket (must be non zero and equal or higher than min_sales)
```

Constructor properties:

```txt
nike_df: Dataframe from NikeScrAPI.getData()
min_sales: minimum ammount of ticket per product per day (can be zero)
max_sales: maximum ammount of ticket per product per day (must be non zero and equal or higher than min_sales)
path: output folder (suggested default value),
chance: chance of not selling an item per day (1/n) chance of occurring (if this occurs the min_sales and max_sales are not applied) the bigger the less likely to occur
```

### Output Files

The new snippet will create the folders with the following structure:

Note: *Assuming data is generated on 2023 Apr 11 for the last 2 days*

```txt
<project root>
  scrapper
    data
      products
        nike_11APR2023_2120.csv
      sales
        2023
          04
            nike_sales_10.csv
            nike_sales_11.csv
```

[terraform_web]: https://developer.hashicorp.com/terraform/downloads
[aws_cli]: https://aws.amazon.com/es/cli/
[aws_profile]: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
[python_web]: https://www.python.org/downloads/
[docker_web]: https://docs.docker.com/get-docker/
[snowflake_web]: https://www.snowflake.com/en/

[file_requirements]: project_requirements.md
