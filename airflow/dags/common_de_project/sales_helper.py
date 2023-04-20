import pandas
import constants
from common_de_project import constants, utils

def get_color_full_uid(uid: str):
    return uid[(-1*constants.uid_len):]

def key_date(date:str, char:str):
    date_split = date.split(char)

    year = date_split[-3]
    month = date_split[-2]
    day = date_split[-1]

    key = utils.hash_key('{year}{month}{day}'.format(
        year = year,
        month = month,
        day = day
    ))
    return key

def get_year(date:str, char:str):
    date_split = date.split(char)
    return date_split[-3]

def get_month(date:str, char:str):
    date_split = date.split(char)
    return date_split[-2]

def get_day(date:str, char:str):
    date_split = date.split(char)
    return date_split[-1]
    

def set_additional_properties(df: pandas.DataFrame):
    df['product_color_id'] = df.apply(lambda row: get_color_full_uid(row['UID']), axis=1)
    df['sales_id'] = df.apply(lambda row: utils.hash_key('{product}{date}'.format(
        product=row['ticket_id'],
        date=row['date']
    )), axis=1)
    df['date_id'] = df.apply(lambda row: key_date(row['date'], '-'), axis=1)
    df['year'] = df.apply(lambda row: get_year(row['date'], '-'), axis=1)
    df['month'] = df.apply(lambda row: get_month(row['date'], '-'), axis=1)
    df['day'] = df.apply(lambda row: get_day(row['date'], '-'), axis=1)
    df['currency_id'] = df.apply(lambda row: utils.hash_key(row['currency']), axis=1)
    return df
