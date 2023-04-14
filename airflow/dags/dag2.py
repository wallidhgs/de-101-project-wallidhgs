
import pandas
from hashlib import md5
from os import environ
from io import StringIO
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from snowflake.connector import connect
from boto3 import client

BUCKET = 'de-project-local'
BUCKET_PREFIX = 'data/'
PRODUCTS_PREFIX = f'{BUCKET_PREFIX}products/'
SALES_PREFIX = f'{BUCKET_PREFIX}sales/'

FILE_PREX = 'nike_'
FILE_EXTENSION = '.csv'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}
s3_client = client(
    's3',
    aws_access_key_id=environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=environ.get('AWS_SECRET_ACCESS_KEY'),
)
snowflake_conn = connect(
    account=environ.get("SNOWFLAKE_ACCOUNT"),
    user=environ.get("SNOWFLAKE_USER"),
    password=environ.get("SNOWFLAKE_PASSWORD"),
    database=environ.get("SNOWFLAKE_DB"),
    schema=environ.get("SNOWFLAKE_SCHEMA"),
    warehouse=environ.get("SNOWFLAKE_WH")
)
cursor = snowflake_conn.cursor()

def read_s3_csv(key:str):
    print(f'Retrieving file: {BUCKET}/{key}')
    csv_obj = s3_client.get_object(Bucket=BUCKET, Key=key)
    body = csv_obj['Body']

    print('Decoding file')
    csv_string = body.read().decode('utf-8')

    print('Loading dataframe from file')
    df = pandas.read_csv(StringIO(csv_string))

    print(df)
    return df
def get_key(val: str):
    val_hash = md5(val.encode())
    val_hex = val_hash.hexdigest()[:20]
    val_key = int(f"0x{val_hex}", 0)
    return val_key
def get_color_full_uid(uid: str, parent_uid: str):
    return uid.replace(parent_uid, '')
def insert_snowflake(df: pandas.DataFrame, table_name: str, columns: list, column_compare: list):
    print(f"Inserting into '{table_name}'...")

    compare_clause_arr = []
    for col in column_compare: compare_clause_arr.append(f't1.{col} = t2.{col}')
    compare_clause = ' AND '.join(compare_clause_arr)

    column_names_arr = []
    col_insert_arr = []
    for col in columns:
        col_insert_arr.append('t2.{name}'.format(name=col['name']))
        column_names_arr.append(col['name'])
    column_names = ",".join(column_names_arr)
    col_insert = ",".join(col_insert_arr)

    for row in df.values.tolist():
        select_arr = []
        for i in range(len(columns)):
            col = columns[i]
            val = row[i]
            if col['wrapper']:
                select_arr.append("'{val}' {name}".format(val=val, name=col['name']))
            else:
                select_arr.append('{val} {name}'.format(val=val, name=col['name']))
        select_query = ",".join(select_arr)

        query = f"""MERGE INTO {table_name} t1
USING (SELECT {select_query}) t2 ON {compare_clause}
WHEN NOT MATCHED THEN INSERT ({column_names}) VALUES ({col_insert});"""

        cursor.execute(query)
    print(f"Insert into '{table_name}' done.")

@task
def load_products_file(s3_file):
    split_uri = s3_file.split('/')
    file_name = split_uri[-1].replace(FILE_PREX, '').replace(FILE_EXTENSION, '')
    print(f'file_name: {file_name}')
    df = read_s3_csv(key=s3_file)
    
    print('adding additional properties...')
    df['color_full_uid'] = df.apply(lambda row: get_color_full_uid(row['UID']), axis=1)

    ret = df.to_csv()
    return ret

@task
def list_products():
    print('Listing product files')
    result = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=PRODUCTS_PREFIX)

    s3_files = []
    for single_file in result['Contents']: s3_files.append(single_file['Key'])
    return s3_files

@task
def load_currency(df_csv):
    print('Parsing data frame')
    df = pandas.read_csv(StringIO(df_csv))
    print('Setting parameters')
    table_name = 'currency_dim'
    columns = [
        {'name': 'id', 'wrapper': False},
        {'name': 'acronym', 'wrapper': True} # wrapper for including "" quotes
    ]
    column_compare = ['id']
    column_names = []
    for col in columns: column_names.append(col['name'])
    print('adding column id')
    column_id_name = 'currency'
    new_column_id_name = 'currency_id'
    df[new_column_id_name] = df.apply(lambda row: get_key(row[column_id_name]), axis=1)
    print('creating filtered dataframe')
    new_df = df.filter(items=[new_column_id_name, column_id_name]).drop_duplicates()
    print('inserting into snowflake')
    insert_snowflake(new_df, table_name, columns, column_compare)
    print('inserted into snowflake')
    return new_df.to_csv()

@task
def load_product_type(df_csv):
    print('Parsing data frame')
    df = pandas.read_csv(StringIO(df_csv))
    print('Setting parameters')
    table_name = 'product_type_dim'
    columns = [
        {'name': 'id', 'wrapper': False},
        {'name': 'name', 'wrapper': True} # wrapper for including "" quotes
    ]
    column_compare = ['id']
    column_names = []
    for col in columns: column_names.append(col['name'])
    print('adding column id')
    column_id_name = 'type'
    new_column_id_name = 'product_type_id'
    df[new_column_id_name] = df.apply(lambda row: get_key(row[column_id_name]), axis=1)
    print('creating filtered dataframe')
    new_df = df.filter(items=[new_column_id_name, column_id_name]).drop_duplicates()
    print('inserting into snowflake')
    insert_snowflake(new_df, table_name, columns, column_compare)
    print('inserted into snowflake')
    return new_df.to_csv()

@task
def load_channel(df_csv):
    print('Parsing data frame')
    df = pandas.read_csv(StringIO(df_csv))
    
    return df.to_csv()
    # print('Setting parameters')
    # table_name = 'product_type_dim'
    # columns = [
    #     {'name': 'id', 'wrapper': False},
    #     {'name': 'name', 'wrapper': True} # wrapper for including "" quotes
    # ]
    # column_compare = ['id']
    # column_names = []
    # for col in columns: column_names.append(col['name'])
    # print('adding column id')
    # column_id_name = 'type'
    # new_column_id_name = 'product_type_id'
    # df[new_column_id_name] = df.apply(lambda row: get_key(row[column_id_name]), axis=1)
    # print('creating filtered dataframe')
    # new_df = df.filter(items=[new_column_id_name, column_id_name]).drop_duplicates()
    # print('inserting into snowflake')
    # insert_snowflake(new_df, table_name, columns, column_compare)
    # print('inserted into snowflake')
    # return new_df.to_csv()
# 
with DAG('de_project_dag3', default_args=default_args, schedule_interval=None) as dag:
    product_list_s3 = list_products()
    df_csvs = load_products_file.expand(s3_file=product_list_s3)
    # currency_dfs = load_currency.expand(df_csv=df_csvs)
    product_type_dfs = load_product_type.expand(df_csv=df_csvs)
    # load_channel.expand(df_csv=df_csvs)
    # load_label.expand(df_csv=df_csvs)

    # load_category.expand(df_csv=df_csvs) # depends on product type

    # load_product.expand(df_csv=df_csvs) # depends on label & category
    # load_product_channel.expand(df_csv=df_csvs) # depends on product & channel
    # load_color.expand(df_csv=df_csvs) # depends on product & currency
