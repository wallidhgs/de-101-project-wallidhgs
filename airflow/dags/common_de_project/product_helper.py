import pandas
import constants
from common_de_project import constants, utils

def get_color_full_uid(uid: str):
    return uid[(-1*constants.uid_len):]

def set_additional_properties(df: pandas.DataFrame):
    df['color_full_uid'] = df.apply(lambda row: get_color_full_uid(row['UID']), axis=1)
    df['currency_id'] = df.apply(lambda row: utils.hash_key(row['currency']), axis=1)
    df['product_type_id'] = df.apply(lambda row: utils.hash_key(row['type']), axis=1)
    df['label_id'] = df.apply(lambda row: utils.hash_key(row['label']), axis=1)
    df['type_id'] = df.apply(lambda row: utils.hash_key(row['type']), axis=1)
    df['category_id'] = df.apply(lambda row: utils.hash_key('{r_category}{r_type}'.format(
        r_category=row['category'],
        r_type=row['type'],
    )), axis=1)
    df['is_main'] = df.apply(lambda row: row['cloudProdID'] == row['color_full_uid'], axis=1)
    return df
