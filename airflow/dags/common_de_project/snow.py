import pandas
from numpy import nan

from os import environ
from snowflake.connector import connect

def getSnowConn():
    snow_conn = connect(
        account=environ.get("SNOWFLAKE_ACCOUNT"),
        user=environ.get("SNOWFLAKE_USER"),
        password=environ.get("SNOWFLAKE_PASSWORD"),
        database=environ.get("SNOWFLAKE_DB"),
        schema=environ.get("SNOWFLAKE_SCHEMA"),
        warehouse=environ.get("SNOWFLAKE_WH")
    )
    return snow_conn

def test():
    print('Creating snow connector')
    conn = getSnowConn()
    print('Creating cursor exists...')
    conn.cursor()
    print('Cursor created')

class HelperCol():
    """
    db_col: Remote column on DB
    csv_col: Local column on source CSV
    wrapper: When generating query column requires " wrapper (let false for bool & numbers)
    is_id: Let false when column is not part of unique identifier
    """
    def __init__(self, db_col: str, csv_col: str, wrapper: bool, is_id: bool):
        self.db_col = db_col
        self.csv_col = csv_col
        self.wrapper = wrapper
        self.is_id = is_id

class Helper():
    """
    self.columns: Array of ColSetting
    self.table: String database table name to be inserted
    """
    def __init__(self, table: str, columns: list):
        self.columns = columns
        self.table = table
    
    def filter_df(self, df: pandas.DataFrame):
        filtered_df_columns = []
        for col in self.columns:
            filtered_df_columns.append(col.csv_col)
        return df.filter(items=filtered_df_columns).drop_duplicates()

    def snowflake_upsert(self, df: pandas.DataFrame, snow_cursor):
        print(f"Upserting into '{self.table}'...")

        compare_clause_arr = []
        column_names_arr = []
        col_insert_arr = []
        for col in self.columns:
            if col.is_id:
                compare_clause_arr.append(f't1.{col.db_col} = t2.{col.db_col}')
            col_insert_arr.append(f't2.{col.db_col}')
            column_names_arr.append(col.db_col)
        compare_clause = ' AND '.join(compare_clause_arr)
        column_names = ",".join(column_names_arr)
        col_insert = ",".join(col_insert_arr)

        for row in df.values.tolist():
            select_arr = []
            for i in range(len(self.columns)):
                col = self.columns[i]
                val = row[i]
                if col.wrapper:
                    if f'{val}' == f'{nan}':
                        val = ''
                    val = f'{val}'.replace("'", '').replace('"', '')
                    select_arr.append(f'\'{val}\' {col.db_col}')
                else:
                    if f'{val}' == f'{nan}':
                        val = 'null'
                    select_arr.append(f'{val} {col.db_col}')

            select_query = ",".join(select_arr)

            query = f"""MERGE INTO {self.table} t1
 USING (SELECT {select_query}) t2 ON {compare_clause}
 WHEN NOT MATCHED THEN INSERT ({column_names}) VALUES ({col_insert});"""
            print(query)
            snow_cursor.execute(query)
        print(f"Inserted into '{self.table}' done.")
