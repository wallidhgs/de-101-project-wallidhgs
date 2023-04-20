# Data Engineering Final Project

*Project requirements are available [here][file_requirements]*

## Prerequirements

- [Python 3.X][python_web]
- [Terraform][terraform_web]
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
- Loading the data into s3 bucket

### Scrapping

*The following commands are used to run the `scrapper/main.py` file this will create the `data/products` and `data/sales` folders with all the raw data required*

From project root:

```sh
cd scrapper
python3 -m venv scrapper_env
pip3 install -r requirements.txt
python3 main.py --max_pages 300
```

> Feel free to use another commands if you already know how to run a py file
> scrapper_env is a suggestion for your virtual environment, however this virtual environment is already excluded in the gitignore file

NOTE: *Architecture supports more than 1 day at once as well as some other configurations, however it was planned to run daily, but it can be tested with more than one day. Check other flags with `--h` option*

```sh
% python3 main.py --h
usage: test.py [-h] [--max_pages MAX_PAGES] [--day_count DAY_COUNT] [--min_sales MIN_SALES] [--max_sales MAX_SALES]

Nike Scraper and Generator launcher

options:
  -h, --help            show this help message and exit
  --max_pages MAX_PAGES
                        Pages to load from NikeScrAPI, for prod use 300
  --day_count DAY_COUNT
                        Days to generates sales records from today to the past, use 0 for 1 day
  --min_sales MIN_SALES
                        Minimum ammount of ticket per product per day (can be zero)
  --max_sales MAX_SALES
                        Maximum ammount of ticket per product per day (must be non zero and equal or higher than min_sales)

# Example:
% python3 main.py --max_pages 300 --day_count 90
```

### Uploading files to S3

> Ensure you have your profile created on `.aws` folder
> The profile name must be `de_project`

If you want to use a different profile update the credentials in the file `terraform/main.tf`:

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
terraform apply tfplan
```

*You will be asked to confirm your command, if getting prompted you will need to type `yes` then hit `Enter` to continue

## Phase 1.5 - Datawarehouse structure

- Login to [snowflake][app_snowflake_web]
- Create the database
  - On the left Panel clic `Data`
  - On the top right click `+ Database`
  - On field `Name` type `DE_PROJECT`
  - Click `Create`
- Create Database Structure
  - On the left Panel clic `Worksheets`
  - On the top right click `+` the `SQL Worksheet`
  - In the middle panel select: Database `DE_PROJECT` and Schema `PUBLIC`
    - After selecting dropdown name should `DE_PROJECT.PUBLIC`
  - On the top right (Left to `Share`) select: Role `ACCOUNTADMIN` Warehouse `COMPUTE_HW`
    - After selecting dropdown name should `ACCOUNTADMIN.COMPUTE_HW`
  - Copy the contents of file `sql/ddl.sql` and paste in the middle panel
  - On the top right (Right to Play button) Click the Dropwdown with the arrow down then click `Run All`

Note: ***COMPUTE_WH** resource is used during the next phase, ensure this resource exists on the Snowflake environment*

## Phase 2-3 - Data Processing & Ingestion

### Data Processing & Ingestion Setup

> On the `Data Processing phase`, use the ingested data from the API and transform accordingly to be ready to be ingested into the Data Warehouse
> On the `Data ingestion into Data Warehouse phase`, lets upload the processed API data into the Data Warehouse

Before launching the app we need to setup some variables (credentials), that will be passed to our environment, using the console execute these commands:

Note: *These are example values, update the values with your credentials*

```sh
export SNOWFLAKE_ACCOUNT=abc123.us-east-1
export SNOWFLAKE_USER=my_user
export SNOWFLAKE_PASSWORD=my_password
```

This phase will use docker in order to provision and run `airflow/dag/etl.py` via airflow

*Before getting started ensure docker daemon is running*, then; from project root use the following commands

Note: *Launch these commands from the same session as you set your Snowflake credentials*

```sh
cd airflow
sh launcher.sh
```

### Data Processing & Ingestion Launch (Running DAG)

When the previous command is complete you sholuld be able to open a browser and navigate to <http://localhost:8080>

- Login with Username `airflow` and Password `airflow`
- You should see listed the DAG `de_project`, click it
- On the top right click the `Play button` the `Trigger DAG`
  - You can optionally click `Graph` tab to see the progress (Click the sections to expand)
- Once it's completed you should see all steps `green` and the status on the top right should be `completed` instead of `running`

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

Results:

- Readme is available on repo (this file)
- File `sql/ddl.sql` available on repo
- File `sql/querys.sql` available on repo

## Considerations (Extra deliverables)

### Scrapper customization

The original requirement suggested the following:

> · The Data Warehouse will be used to calculate the sales of the company, the sales will be calculated as:
> · For all the products that the API ingested, sales will be the sum of the Current Price

However, this was not ideal as only 1 sale per product will be created; instead an additional script was created for the scrapper that can create a wide variety of random sales on the `scrapper/sales_generator.py` file

The original snippet was updated in `scrapper/main.py` and also `scrapper/nikescrapi.py` was cleaned to remove unused imports, unused methods and make private some public methods that were only internally used

#### Sales generator constraints

Private properties:
*These properties are static in the class*

```txt
min_qty: Minimum items per ticket (must not be zero)
max_qty: Maximum items per ticket (must be non zero and equal or higher than min_qty)
min_index: Minimum random for ticket_id (must not be zero)
max_index: Maximum random for ticket_id (must be non zero and equal or higher than min_index)
```

Constructor properties:

```txt
nike_df: Dataframe from NikeScrAPI.getData()
min_sales: minimum ammount of ticket per product per day (can be zero)
max_sales: maximum ammount of ticket per product per day (must be non zero and equal or higher than min_sales)
path: output folder (suggested default value),
chance: chance of not selling an item per day (1/n) chance of occurring (if this occurs the min_sales and max_sales are not applied) the bigger the less likely to occur
```

#### Output Files

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

### DAG Additionals

- The local data is renamed after successful run (not before to let the developer debug the data if error occurs)
- The DAG will create multiple values as reference (id's), these are unique hashes for each row, which means you can run the DAG with the same information multiple times (UPSERT)
- The DAG was intended for loading only 1 day of sales, since it was very likely in a production environment data is received once per day, however it supports multiple days at once
- An additional DAG `de_project_paralel` is provided, these is the same DAG as `de_project`, but the steps for upsert are executed in paralel
  - These DAG ignores FK (currently disabled on script), these could be faster but no safer if planning enabling FK check in the future, check: `sql/ddl.sql`

## Future implementations

- Integrate CI/CD with `terraform` for `snowflake`
- Implement `RSA key` file for Snowflake instead of `ENV` credentials
- Enable support for `terraform destroy` without manually emptying the bucket
- Enable support to run `DAG with configuration` to run on specific date
- Enable terraform support for running on `gh actions`
  - Manual trigger
  - Trigger on commit
- Enable scheduling daily with `today` value for DAG on `airflow`
- Enable scheduling daily with `today` value for Scrapper on `gh actions`
- Send slack notification for Scrapper `gh actions` and `dag` triggered/result
- Enable paralel loading with pyspark and external scripts (Shell Operators or Remote Jobs)
- Enable cache for file loading
- Upload intermediate data to S3
  - Clear local data after success on S3 upload

## Useful links

[Download Terraform][terraform_web]
[Download AWS CLI][aws_cli]
[Setup CLI Profile][aws_profile]
[Download Python][python_web]
[Download Docker][docker_web]
[Snowflake Main Page][snowflake_web]
[Login Snowflake][app_snowflake_web]
[Original Requirements][file_requirements]

[terraform_web]: https://developer.hashicorp.com/terraform/downloads
[aws_cli]: https://aws.amazon.com/es/cli/
[aws_profile]: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
[python_web]: https://www.python.org/downloads/
[docker_web]: https://docs.docker.com/get-docker/
[snowflake_web]: https://www.snowflake.com/en/
[app_snowflake_web]: https://app.snowflake.com

[file_requirements]: project_requirements.md
