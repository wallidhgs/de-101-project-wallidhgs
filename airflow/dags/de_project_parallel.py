import pandas

from ast import literal_eval
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator

# We need to add this to Docker path
import sys
sys.path.append('/opt/airflow/dags/common_de_project')
from common_de_project import constants, s3, snow, utils, product_helper, sales_helper

s3_client = s3.get_client()

snow_conn = snow.getSnowConn()
snow_cursor = snow_conn.cursor()

def check_connections():
    s3.test()
    snow.test()

# Products
def products_list():
    print('Listing files')
    files = s3.list_products_files(s3_client=s3_client)
    print('Filenames obtained')
    print(files)
    utils.save_array_file(arr=files, path=constants.tmp_list_products_file)
    print(f'Filenames saved to {constants.tmp_list_products_file}')

def download_products_files():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_products_file)
    print('Iterating')
    for s3_file in s3_files:
        print(f'Buffering file: {s3_file}')
        df = s3.read_csv(key=s3_file, s3_client=s3_client)

        print('Adding additional properties...')
        df = product_helper.set_additional_properties(df)
        
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Saving new file: {csv_path}')

        utils.create_file_path(path=csv_path)
        df.to_csv(csv_path, index=False)

def handle_currency():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_products_file)
    print('Iterating')
    for s3_file in s3_files:
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Buffering file: {csv_path}')
        df = pandas.read_csv(csv_path)
        helper = snow.Helper(
            table='currency_dim',
            columns=[
                snow.HelperCol(
                    db_col='id',
                    csv_col='currency_id',
                    wrapper=False,
                    is_id=True
                ),
                snow.HelperCol(
                    db_col='acronym',
                    csv_col='currency',
                    wrapper=True,
                    is_id=False
                )
            ]
        )
        df = helper.filter_df(df)
        helper.snowflake_upsert(df=df, snow_cursor=snow_cursor)

def handle_product_type():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_products_file)
    print('Iterating')
    for s3_file in s3_files:
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Buffering file: {csv_path}')
        df = pandas.read_csv(csv_path)
        helper = snow.Helper(
            table='product_type_dim',
            columns=[
                snow.HelperCol(
                    db_col='id',
                    csv_col='product_type_id',
                    wrapper=False,
                    is_id=True
                ),
                snow.HelperCol(
                    db_col='name',
                    csv_col='type',
                    wrapper=True,
                    is_id=False
                )
            ]
        )
        df = helper.filter_df(df)
        helper.snowflake_upsert(df=df, snow_cursor=snow_cursor)

def handle_channel():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_products_file)
    print('Iterating')
    for s3_file in s3_files:
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Buffering file: {csv_path}')
        df = pandas.read_csv(csv_path)
        
        df = df.filter(items=['channel']).drop_duplicates()

        new_df = pandas.DataFrame([], columns=['id', 'name'])
        for _, row in df.iterrows():
            channels = literal_eval(row['channel'])
            for channel in channels: new_df.loc[len(new_df)] = [utils.hash_key(channel), channel]
        new_df = new_df.drop_duplicates()

        helper = snow.Helper(
            table='channel_dim',
            columns=[
                snow.HelperCol(
                    db_col='id',
                    csv_col='', # We dont need this as df is already filtered
                    wrapper=False,
                    is_id=True
                ),
                snow.HelperCol(
                    db_col='name',
                    csv_col='', # We dont need this as df is already filtered
                    wrapper=True,
                    is_id=False
                )
            ]
        )

        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_channel)
        print(f'Saving new file: {csv_path}')
        utils.create_file_path(path=csv_path)
        new_df.to_csv(csv_path, index=False)

        helper.snowflake_upsert(df=new_df, snow_cursor=snow_cursor)

def handle_label():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_products_file)
    print('Iterating')
    for s3_file in s3_files:
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Buffering file: {csv_path}')
        df = pandas.read_csv(csv_path)
        helper = snow.Helper(
            table='label_dim',
            columns=[
                snow.HelperCol(
                    db_col='id',
                    csv_col='label_id',
                    wrapper=False,
                    is_id=True
                ),
                snow.HelperCol(
                    db_col='name',
                    csv_col='label',
                    wrapper=True,
                    is_id=False
                )
            ]
        )
        df = helper.filter_df(df)
        helper.snowflake_upsert(df=df, snow_cursor=snow_cursor)

def handle_category():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_products_file)
    print('Iterating')
    for s3_file in s3_files:
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Buffering file: {csv_path}')
        df = pandas.read_csv(csv_path)
        helper = snow.Helper(
            table='category_dim',
            columns=[
                snow.HelperCol(
                    db_col='id',
                    csv_col='category_id',
                    wrapper=False,
                    is_id=True
                ),
                snow.HelperCol(
                    db_col='name',
                    csv_col='category',
                    wrapper=True,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='product_type',
                    csv_col='type_id',
                    wrapper=False,
                    is_id=False
                ),
            ]
        )
        df = helper.filter_df(df)
        helper.snowflake_upsert(df=df, snow_cursor=snow_cursor)

def handle_product():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_products_file)
    print('Iterating')
    for s3_file in s3_files:
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Buffering file: {csv_path}')
        df = pandas.read_csv(csv_path)
        helper = snow.Helper(
            table='product_dim',
            columns=[
                snow.HelperCol(
                    db_col='id',
                    csv_col='cloudProdID',
                    wrapper=True,
                    is_id=True
                ),
                snow.HelperCol(
                    db_col='title',
                    csv_col='title',
                    wrapper=True,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='subtitle',
                    csv_col='subtitle',
                    wrapper=True,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='short_description',
                    csv_col='short_description',
                    wrapper=True,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='discount',
                    csv_col='sale',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='rating',
                    csv_col='rating',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='customizable',
                    csv_col='customizable',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='extended_sizing',
                    csv_col='ExtendedSizing',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='gift_card',
                    csv_col='GiftCard',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='jersey',
                    csv_col='Jersey',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='nba',
                    csv_col='NBA',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='nfl',
                    csv_col='NFL',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='sustainable',
                    csv_col='Sustainable',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='url',
                    csv_col='prod_url',
                    wrapper=True,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='category',
                    csv_col='category_id',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='label',
                    csv_col='label_id',
                    wrapper=False,
                    is_id=False
                )
            ]
        )
        df = helper.filter_df(df)
        helper.snowflake_upsert(df=df, snow_cursor=snow_cursor)

def handle_product_channel():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_products_file)
    print('Iterating')
    for s3_file in s3_files:
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Buffering file: {csv_path}')
        df = pandas.read_csv(csv_path)

        df = df.filter(items=['channel', 'cloudProdID']).drop_duplicates()

        new_df = pandas.DataFrame([], columns=['channel', 'product'])
        for _, row in df.iterrows():
            channels = literal_eval(row['channel'])
            for channel in channels: new_df.loc[len(new_df)] = [utils.hash_key(channel), row['cloudProdID']]
        new_df = new_df.drop_duplicates()


        helper = snow.Helper(
            table='product_channel_dim',
            columns=[
                snow.HelperCol(
                    db_col='channel',
                    csv_col='', # We dont need this as df is already filtered
                    wrapper=False,
                    is_id=True
                ),
                snow.HelperCol(
                    db_col='product',
                    csv_col='', # We dont need this as df is already filtered
                    wrapper=True,
                    is_id=True
                )
            ]
        )

        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_product_channel)
        print(f'Saving new file: {csv_path}')
        utils.create_file_path(path=csv_path)
        new_df.to_csv(csv_path, index=False)

        helper.snowflake_upsert(df=new_df, snow_cursor=snow_cursor)

def handle_color():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_products_file)
    print('Iterating')
    for s3_file in s3_files:
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Buffering file: {csv_path}')
        df = pandas.read_csv(csv_path)
        helper = snow.Helper(
            table='color_dim',
            columns=[
                snow.HelperCol(
                    db_col='id',
                    csv_col='color_full_uid',
                    wrapper=True,
                    is_id=True
                ),
                snow.HelperCol(
                    db_col='offline_id',
                    csv_col='productID',
                    wrapper=True,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='short_id',
                    csv_col='shortID',
                    wrapper=True,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='is_main',
                    csv_col='is_main',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='description',
                    csv_col='color-Description',
                    wrapper=True,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='color_num',
                    csv_col='colorNum',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='full_price',
                    csv_col='color-FullPrice',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='current_price',
                    csv_col='color-CurrentPrice',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='in_stock',
                    csv_col='color-InStock',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='coming_soon',
                    csv_col='ComingSoon',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='best_seller',
                    csv_col='BestSeller',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='excluded',
                    csv_col='Excluded',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='launch',
                    csv_col='Launch',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='member_exclusive',
                    csv_col='MemberExclusive',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='pre_build_id',
                    csv_col='prebuildId',
                    wrapper=True,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='is_new',
                    csv_col='color-New',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='image_url',
                    csv_col='color-Image-url',
                    wrapper=True,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='product',
                    csv_col='cloudProdID',
                    wrapper=True,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='currency',
                    csv_col='currency_id',
                    wrapper=False,
                    is_id=False
                )
            ]
        )
        df = helper.filter_df(df)
        helper.snowflake_upsert(df=df, snow_cursor=snow_cursor)

# Sales
def sales_list():
    print('Listing files')
    files = s3.list_sales_files(s3_client=s3_client)
    print('Filenames obtained')
    print(files)
    utils.save_array_file(arr=files, path=constants.tmp_list_sales_file)
    print(f'Filenames saved to {constants.tmp_list_sales_file}')

def download_sales_files():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_sales_file)
    print('Iterating')
    for s3_file in s3_files:
        print(f'Buffering file: {s3_file}')
        df = s3.read_csv(key=s3_file, s3_client=s3_client)

        print('Adding additional properties...')
        df = sales_helper.set_additional_properties(df)
        
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Saving new file: {csv_path}')

        utils.create_file_path(path=csv_path)
        df.to_csv(csv_path, index=False)

def handle_dates():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_sales_file)
    print('Iterating')
    for s3_file in s3_files:
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Buffering file: {csv_path}')
        df = pandas.read_csv(csv_path)
        helper = snow.Helper(
            table='date_dim',
            columns=[
                snow.HelperCol(
                    db_col='id',
                    csv_col='date_id',
                    wrapper=False,
                    is_id=True
                ),
                snow.HelperCol(
                    db_col='year',
                    csv_col='year',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='month',
                    csv_col='month',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='day',
                    csv_col='day',
                    wrapper=False,
                    is_id=False
                )
            ]
        )
        df = helper.filter_df(df)
        helper.snowflake_upsert(df=df, snow_cursor=snow_cursor)

def handle_sales_fact():
    print('Reading file list')
    s3_files = utils.read_array_file(path=constants.tmp_list_sales_file)
    print('Iterating')
    for s3_file in s3_files:
        csv_path = utils.get_datalake_local_file_name(s3_uri=s3_file, lake_name=constants.csv_additional_properties)
        print(f'Buffering file: {csv_path}')
        df = pandas.read_csv(csv_path)

        helper = snow.Helper(
            table='sales_fact',
            columns=[
                snow.HelperCol(
                    db_col='id',
                    csv_col='sales_id',
                    wrapper=False,
                    is_id=True
                ),
                snow.HelperCol(
                    db_col='ticket_id',
                    csv_col='ticket_id',
                    wrapper=False,
                    is_id=True
                ),
                snow.HelperCol(
                    db_col='sales',
                    csv_col='sales',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='quantity',
                    csv_col='quantity',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='currency',
                    csv_col='currency_id',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='date',
                    csv_col='date_id',
                    wrapper=False,
                    is_id=False
                ),
                snow.HelperCol(
                    db_col='product',
                    csv_col='product_color_id',
                    wrapper=True,
                    is_id=False
                )
            ]
        )
        df = helper.filter_df(df)
        helper.snowflake_upsert(df=df, snow_cursor=snow_cursor)

with DAG(dag_id="de_project_parallel", default_args=constants.dag_default_args, schedule_interval=None, tags=["de_project"]) as dag:
    connection_check = PythonOperator(
        task_id='check_connections',
        python_callable=check_connections,
        dag=dag
    )

    # Product
    get_products_list = PythonOperator(
        task_id='get_product_list',
        python_callable=products_list,
        dag=dag
    )
    set_product_additional_properties = PythonOperator(
        task_id='download_products_files',
        python_callable=download_products_files,
        dag=dag
    )
    with TaskGroup("load_products_catalogs", tooltip="Loading Products") as load_products_catalogs:
        load_currency = PythonOperator(
            task_id='handle_currency',
            python_callable=handle_currency,
            dag=dag
        )
        load_product_type = PythonOperator(
            task_id='handle_product_type',
            python_callable=handle_product_type,
            dag=dag
        )
        load_channel = PythonOperator(
            task_id='handle_channel',
            python_callable=handle_channel,
            dag=dag
        )
        load_label = PythonOperator(
            task_id='handle_label',
            python_callable=handle_label,
            dag=dag
        )
        load_category = PythonOperator(
            task_id='handle_category',
            python_callable=handle_category,
            dag=dag
        )
        load_product = PythonOperator(
            task_id='handle_product',
            python_callable=handle_product,
            dag=dag
        )
        load_product_channel = PythonOperator(
            task_id='handle_product_channel',
            python_callable=handle_product_channel,
            dag=dag
        )
        load_color = PythonOperator(
            task_id='handle_color',
            python_callable=handle_color,
            dag=dag
        )

    # Sales
    get_sales_list = PythonOperator(
        task_id='get_sales_list',
        python_callable=sales_list,
        dag=dag
    )
    set_sales_additional_properties = PythonOperator(
        task_id='download_sales_files',
        python_callable=download_sales_files,
        dag=dag
    )
    with TaskGroup("load_sales_catalogs", tooltip="Loading Sales") as load_sales_catalogs:
        load_dates = PythonOperator(
            task_id='handle_dates',
            python_callable=handle_dates,
            dag=dag
        )
        load_sales_fact = PythonOperator(
            task_id='handle_sales_fact',
            python_callable=handle_sales_fact,
            dag=dag
        )
    
    # Orchestrator
    connection_check >> get_products_list >> set_product_additional_properties >> load_products_catalogs
    connection_check >> get_sales_list >> set_sales_additional_properties >> load_sales_catalogs
