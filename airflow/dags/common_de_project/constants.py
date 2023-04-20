from datetime import datetime, timedelta

dag_default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

# S3
bucket = 'de-project-local'
bucket_folder_prefix = 'data/'
products_folder_prefix = f'{bucket_folder_prefix}products/'
products_file_prefix = 'nike_'
sales_file_prefix = 'nike_sales_'
sales_folder_prefix = f'{bucket_folder_prefix}sales/'
file_extension = '.csv'

# Processing
uid_len = 36

tmp_folder = '/opt/airflow/tmp/'
tmp_list_products_file = f'{tmp_folder}products_list.txt'
tmp_list_sales_file = f'{tmp_folder}sales_list.txt'

csv_additional_properties = 'additional_properties'
csv_channel = 'channel'
csv_product_channel = 'product_channel'
