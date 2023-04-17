
import pandas
from ast import literal_eval
from numpy import nan
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

PRODUCT_FILE_PREFIX = 'nike_'
SALES_FILE_PREFIX = 'nike_sales_'
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
def get_color_full_uid(uid: str):
    return uid[-36:]
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
                if f'{val}' == f'{nan}':
                    val = ''
                val = val.replace("'", '').replace('"', '')
                select_arr.append("'{val}' {name}".format(val=val, name=col['name']))
            else:
                if f'{val}' == f'{nan}':
                    val = 'null'
                select_arr.append('{val} {name}'.format(val=val, name=col['name']))
        select_query = ",".join(select_arr)

        query = f"""MERGE INTO {table_name} t1
USING (SELECT {select_query}) t2 ON {compare_clause}
WHEN NOT MATCHED THEN INSERT ({column_names}) VALUES ({col_insert});"""
        print(query)
        cursor.execute(query)
    print(f"Insert into '{table_name}' done.")

@task
def load_products_file(s3_file):
    split_uri = s3_file.split('/')
    file_name = split_uri[-1].replace(PRODUCT_FILE_PREFIX, '').replace(FILE_EXTENSION, '')
    print(f'file_name: {file_name}')
    df = read_s3_csv(key=s3_file)
    
    print('adding additional properties...')
    df['color_full_uid'] = df.apply(lambda row: get_color_full_uid(row['UID']), axis=1)
    df['currency_id'] = df.apply(lambda row: get_key(row['currency']), axis=1)
    df['product_type_id'] = df.apply(lambda row: get_key(row['type']), axis=1)
    df['label_id'] = df.apply(lambda row: get_key(row['label']), axis=1)
    df['category_id'] = df.apply(lambda row: get_key('{r_category}{r_type}'.format(
        r_category=row['category'],
        r_type=row['type'],
    )), axis=1)
    df['type_id'] = df.apply(lambda row: get_key(row['type']), axis=1)
    df['is_main'] = df.apply(lambda row: row['cloudProdID'] == row['color_full_uid'], axis=1)

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
        {'name': 'acronym', 'wrapper': True}
    ]
    column_compare = ['id']
    column_names = []
    for col in columns: column_names.append(col['name'])
    print('creating filtered dataframe')
    new_df = df.filter(items=['currency_id', 'currency']).drop_duplicates()
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
        {'name': 'name', 'wrapper': True}
    ]
    column_compare = ['id']
    column_names = []
    for col in columns: column_names.append(col['name'])
    print('creating filtered dataframe')
    new_df = df.filter(items=['product_type_id', 'type']).drop_duplicates()
    print('inserting into snowflake')
    insert_snowflake(new_df, table_name, columns, column_compare)
    print('inserted into snowflake')
    return new_df.to_csv()

@task
def load_channel(df_csv):
    print('Parsing data frame')
    df = pandas.read_csv(StringIO(df_csv))
    
    filter_df = df.filter(items=['channel']).drop_duplicates()

    print('splitting channel')
    column_names = ['id', 'name']
    new_df = pandas.DataFrame([], columns=column_names)
    for _, row in filter_df.iterrows():
        channels = literal_eval(row['channel'])
        for channel in channels:
            new_df.loc[len(new_df)] = [
                get_key(channel),
                channel
            ]
    new_df = new_df.drop_duplicates()
    
    print('Setting parameters')
    table_name = 'channel_dim'
    columns = [
        {'name': 'id', 'wrapper': False},
        {'name': 'name', 'wrapper': True}
    ]
    column_compare = ['id']

    print('inserting into snowflake')
    insert_snowflake(new_df, table_name, columns, column_compare)
    print('inserted into snowflake')
    return new_df.to_csv()

@task
def load_label(df_csv):
    print('Parsing data frame')
    df = pandas.read_csv(StringIO(df_csv))
    print('Setting parameters')
    table_name = 'label_dim'
    columns = [
        {'name': 'id', 'wrapper': False},
        {'name': 'name', 'wrapper': True}
    ]
    column_compare = ['id']
    column_names = []
    for col in columns: column_names.append(col['name'])
    print('creating filtered dataframe')
    new_df = df.filter(items=['label_id', 'label']).drop_duplicates()
    print('inserting into snowflake')
    insert_snowflake(new_df, table_name, columns, column_compare)
    print('inserted into snowflake')
    return new_df.to_csv()

@task
def load_category(df_csv):
    print('Parsing data frame')
    df = pandas.read_csv(StringIO(df_csv))
    print('Setting parameters')
    table_name = 'category_dim'
    columns = [
        {'name': 'id', 'wrapper': False},
        {'name': 'name', 'wrapper': True},
        {'name': 'product_type', 'wrapper': False} # FK with product type
    ]
    column_compare = ['id']
    column_names = []
    for col in columns: column_names.append(col['name'])
    print('creating filtered dataframe')
    new_df = df.filter(items=['category_id', 'category', 'type_id']).drop_duplicates()
    print('inserting into snowflake')
    insert_snowflake(new_df, table_name, columns, column_compare)
    print('inserted into snowflake')
    return new_df.to_csv()

@task
def load_product(df_csv):
    print('Parsing data frame')
    df = pandas.read_csv(StringIO(df_csv))
    print('Setting parameters')
    table_name = 'product_dim'
    columns = [
        {'name': 'id', 'wrapper': True},
        {'name': 'title', 'wrapper': True},
        {'name': 'subtitle', 'wrapper': True},
        {'name': 'short_description', 'wrapper': True},
        {'name': 'discount', 'wrapper': False},
        {'name': 'rating', 'wrapper': False},
        {'name': 'customizable', 'wrapper': False},
        {'name': 'extended_sizing', 'wrapper': False},
        {'name': 'gift_card', 'wrapper': False},
        {'name': 'jersey', 'wrapper': False},
        {'name': 'nba', 'wrapper': False},
        {'name': 'nfl', 'wrapper': False},
        {'name': 'sustainable', 'wrapper': False},
        {'name': 'url', 'wrapper': True},

        {'name': 'category', 'wrapper': False},
        {'name': 'label', 'wrapper': False},
    ]
    column_compare = ['id']
    column_names = []
    for col in columns: column_names.append(col['name'])
    print('creating filtered dataframe')
    new_df = df.filter(items=[
        'cloudProdID',
        'title',
        'subtitle',
        'short_description',
        'sale',
        'rating',
        'customizable',
        'ExtendedSizing',
        'GiftCard',
        'Jersey',
        'NBA',
        'NFL',
        'Sustainable',
        'prod_url',

        'category_id',
        'label_id'
    ]).drop_duplicates()
    print('inserting into snowflake')
    insert_snowflake(new_df, table_name, columns, column_compare)
    print('inserted into snowflake')
    return new_df.to_csv()

@task
def load_product_channel(df_csv):
    print('Parsing data frame')
    df = pandas.read_csv(StringIO(df_csv))
    
    filter_df = df.filter(items=['channel', 'cloudProdID']).drop_duplicates()

    print('splitting channel')
    column_names = ['channel', 'product']
    new_df = pandas.DataFrame([], columns=column_names)
    for _, row in filter_df.iterrows():
        channels = literal_eval(row['channel'])
        for channel in channels:
            new_df.loc[len(new_df)] = [
                get_key(channel),
                row['cloudProdID']
            ]
    new_df = new_df.drop_duplicates()
    
    print('Setting parameters')
    table_name = 'product_channel_dim'
    columns = [
        {'name': 'channel', 'wrapper': False},
        {'name': 'product', 'wrapper': True}
    ]
    column_compare = ['channel', 'product']

    print('inserting into snowflake')
    insert_snowflake(new_df, table_name, columns, column_compare)
    print('inserted into snowflake')
    return new_df.to_csv()

@task
def load_color(df_csv):
    print('Parsing data frame')
    df = pandas.read_csv(StringIO(df_csv))
    print('Setting parameters')
    table_name = 'color_dim'
    columns = [
        {'name': 'id', 'wrapper': True},
        {'name': 'offline_id', 'wrapper': True},
        {'name': 'short_id', 'wrapper': True},
        {'name': 'is_main', 'wrapper': False},
        {'name': 'description', 'wrapper': True},
        {'name': 'color_num', 'wrapper': False},
        {'name': 'full_price', 'wrapper': False},
        {'name': 'current_price', 'wrapper': False},
        {'name': 'in_stock', 'wrapper': False},
        {'name': 'coming_soon', 'wrapper': False},
        {'name': 'best_seller', 'wrapper': False},
        {'name': 'excluded', 'wrapper': False},
        {'name': 'launch', 'wrapper': False},
        {'name': 'member_exclusive', 'wrapper': False},
        {'name': 'pre_build_id', 'wrapper': True}, # -> Check empty
        {'name': 'is_new', 'wrapper': False},
        {'name': 'image_url', 'wrapper': True},
        {'name': 'product', 'wrapper': True},
        {'name': 'currency', 'wrapper': False}
    ]
    column_compare = ['id']
    column_names = []
    for col in columns: column_names.append(col['name'])
    print('creating filtered dataframe')
    new_df = df.filter(items=[
        'color_full_uid',
        'productID',
        'shortID',
        'is_main',
        'color-Description',
        'colorNum',
        'color-FullPrice',
        'color-CurrentPrice',
        'color-InStock',
        'ComingSoon',
        'BestSeller',
        'Excluded',
        'Launch',
        'MemberExclusive',
        'prebuildId',
        'color-New',
        'color-Image-url',
        'cloudProdID',
        'currency_id'
    ]).drop_duplicates()
    print('inserting into snowflake')
    insert_snowflake(new_df, table_name, columns, column_compare)
    print('inserted into snowflake')
    return new_df.to_csv()

########################################

@task
def sales_list_s3():
    print('Listing product files')
    result = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=SALES_PREFIX)

    s3_files = []
    for single_file in result['Contents']: s3_files.append(single_file['Key'])
    return s3_files

@task
def load_sales_file(s3_file):
    split_uri = s3_file.split('/')
    file_name = split_uri[-1].replace(SALES_FILE_PREFIX, '').replace(FILE_EXTENSION, '')
    print(f'file_name: {file_name}')
    df = read_s3_csv(key=s3_file)
    
    print('adding additional properties...')
    df['product_color_id'] = df.apply(lambda row: get_color_full_uid(row['UID']), axis=1)
    df['sales_id'] = df.apply(lambda row: get_key('{product}{date}'.format(
        product=row['UID'],
        date=row['date']
    )), axis=1)
    df['date_id'] = df.apply(lambda row: key_date(row['date'], '-'), axis=1)

    ret = df.to_csv()
    return ret

@task
def load_date(s3_file):
    table_name = 'date_dim'
    columns = [
        {'name': 'id', 'wrapper': False},
        {'name': 'year', 'wrapper': False},
        {'name': 'month', 'wrapper': False},
        {'name': 'day', 'wrapper': False}
    ]
    column_compare = ['id']

    split_uri = s3_file.split('/')
    file_name = split_uri[-1].replace(SALES_FILE_PREFIX, '').replace(FILE_EXTENSION, '')
    print(f'file_name: {file_name}')

    file_name_split = file_name.split('_')

    year = file_name_split[-3]
    month = file_name_split[-2]
    day = file_name_split[-1]

    column_names = []
    for col in columns: column_names.append(col['name'])

    key = get_key('{year}{month}{day}'.format(
        year = year,
        month = month,
        day = day
    ))
    df = pandas.DataFrame([(key, year, month, day)], columns=column_names)
    insert_snowflake(df, table_name, columns, column_compare)
    return df.to_csv()

def key_date(date, char):
    date_split = date.split(char)

    year = date_split[-3]
    month = date_split[-2]
    day = date_split[-1]
    

    key = get_key('{year}{month}{day}'.format(
        year = year,
        month = month,
        day = day
    ))
    return key
    
@task
def load_sale(df_csv):
    print('Parsing data frame')
    df = pandas.read_csv(StringIO(df_csv))
    print('Setting parameters')
    table_name = 'sales_fact'
    columns = [
        {'name': 'id', 'wrapper': False},
        {'name': 'sales', 'wrapper': False},
        {'name': 'date', 'wrapper': False},
        {'name': 'product', 'wrapper': True} # FK
    ]
    column_compare = ['id']
    column_names = []
    for col in columns: column_names.append(col['name'])
    print('creating filtered dataframe')
    new_df = df.filter(items=['sales_id', 'sales', 'date_id', 'product_color_id'])
    print('inserting into snowflake')
    insert_snowflake(new_df, table_name, columns, column_compare)
    print('inserted into snowflake')
    return new_df.to_csv()

with DAG('a_de_project_dag3', default_args=default_args, schedule_interval=None) as dag:
    product_list_s3 = list_products()
    product_df_csvs = load_products_file.expand(s3_file=product_list_s3)
    currency_dfs = load_currency.expand(df_csv=product_df_csvs)
    product_type_dfs = load_product_type.expand(df_csv=product_df_csvs)
    channel_dfs = load_channel.expand(df_csv=product_df_csvs)
    label_dfs = load_label.expand(df_csv=product_df_csvs)
    
    # Since FK are deactivated in snowflake, these can be created in paralel threads
    category_dfs = load_category.expand(df_csv=product_df_csvs) # depends on product type
    product_dfs = load_product.expand(df_csv=product_df_csvs) # depends on label & category
    load_product_channel.expand(df_csv=product_df_csvs) # depends on product & channel
    load_color.expand(df_csv=product_df_csvs) # depends on product & currency
    ############################################
    sales_list_s3 = sales_list_s3()
    sales_df_csvs = load_sales_file.expand(s3_file=sales_list_s3)
    date_csvs = load_date.expand(s3_file=sales_list_s3)
    sales_csvs = load_sale.expand(df_csv=sales_df_csvs) # depends on date
