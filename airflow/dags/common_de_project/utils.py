import os
import constants

from hashlib import md5

def hash_key(val: str):
    val_hash = md5(val.encode())
    val_hex = val_hash.hexdigest()[:20]
    val_key = int(f"0x{val_hex}", 0)
    return val_key

def create_folder(path: str):
    print(f'Creating path: {path}...')
    if not os.path.exists(path):
        os.makedirs(path)
        print(f'Path: {path} created')
        return
    print(f'Path: {path} already exists.')

def create_file_path(path: str):
    print(f'Creating path for file: {path}...')
    fullpath = os.path.dirname(path)
    create_folder(fullpath)

def save_array_file(arr: list, path: str):
    with open(path, 'w') as filehandle:
        for line in arr:
            filehandle.write(f"{line}\n")

def read_array_file(path: str):
    with open(path, 'r') as filehandle:
        data = filehandle.readlines()
        for i in range(len(data)):
            data[i] = data[i].replace('\n', '').replace('\r', '') # Removing Enters from returned array
        return data

def get_datalake_local_file_name(s3_uri: str, lake_name: str):
    split_uri = s3_uri.split('/')
    file_name = split_uri[-1]
    local_path = "/".join(split_uri[:-1])
    data_lake_file_name = f'{constants.tmp_folder}{local_path}/{lake_name}/{file_name}'
    return data_lake_file_name
