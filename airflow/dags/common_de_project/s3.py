import constants
import utils
import pandas

from os import environ
from boto3 import client
from io import StringIO

def get_client():
    s3_client = client(
        's3',
        aws_access_key_id=environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=environ.get('AWS_SECRET_ACCESS_KEY'),
    )
    return s3_client

def read_csv(key: str, s3_client):
    print(f"Retrieving S3 file: Bucket='{constants.bucket}' Key='{key}'")
    csv_obj = s3_client.get_object(Bucket=constants.bucket, Key=key)
    body = csv_obj['Body']

    print('Decoding file')
    csv_string = body.read().decode('utf-8')

    print('Loading dataframe from file')
    df = pandas.read_csv(StringIO(csv_string))

    print(df)
    return df

def list_products_files(s3_client):
    print('Listing product files')
    result = s3_client.list_objects_v2(
        Bucket=constants.bucket,
        Prefix=constants.products_folder_prefix
    )

    s3_files = []
    for single_file in result['Contents']: s3_files.append(single_file['Key'])
    return s3_files

def list_sales_files(s3_client):
    print('Listing sales files')
    result = s3_client.list_objects_v2(
        Bucket=constants.bucket,
        Prefix=constants.sales_folder_prefix
    )

    s3_files = []
    for single_file in result['Contents']: s3_files.append(single_file['Key'])
    return s3_files

def test():
    print('Connecting to S3')
    client = get_client()
    print(f'Checking bucket "{constants.bucket}" exists...')
    client.head_bucket(Bucket=constants.bucket)
    print(f'Bucket "{constants.bucket}" exists!!!')

def get_csv_name(s3_uri: str):
    split_uri = s3_uri.split('/')
    file_name = split_uri[-1].replace(constants.products_file_prefix, '').replace(constants.file_extension, '')
    return file_name
