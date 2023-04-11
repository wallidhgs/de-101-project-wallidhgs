from datetime import date, timedelta
import pandas
import random
import os

class SalesGenerator():
    __min_qty = 1
    __max_qty = 5
    __column_names = ['UID', 'currency', 'sales', 'quantity', 'date']
    __file_prefix = 'nike_sales_'

    def __init__(self,
                 nike_df: pandas.DataFrame,
                 min_sales: int,
                 max_sales: int,
                 path='data/sales',
                 chance=2):
        print('\nnike_df')
        print(nike_df)
        print('\n\n')
        self.__df = nike_df
        self.__min = min_sales
        self.__max = max_sales
        self.__path = path
        self.__chance = chance  # chance of a record of being generated 1/n for every product

    def __generate_day(self, day: date):
        df = pandas.DataFrame([], columns=self.__column_names)
        for index, row in self.__df.iterrows():
            chance = random.randint(1, self.__chance)
            if (chance == self.__chance):
                sales = random.randint(self.__min, self.__max)
                for n in range(sales):
                    qty = random.randint(self.__min_qty, self.__max_qty)
                    df.loc[len(df)] = [
                        row['UID'],
                        row['currency'],
                        row['currentPrice'] * qty,
                        qty,
                        day.strftime('%Y-%m-%d')
                    ]
        return df
 
    def __create_folders(self, date: date):
        path = '{path}/{date_folder}'.format(
            path=self.__path,
            date_folder=date.strftime('%Y/%m')
            )
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def generate_interval(self, start: date, end: date):
        day_count = (end - start).days + 1
        for single_date in (start + timedelta(n) for n in range(day_count)):
          df = self.__generate_day(single_date)
          file_name="{}{}.csv".format(self.__file_prefix, single_date.strftime('%d'))
          path = self.__create_folders(single_date)
          file_full_path = os.path.join(path,file_name)
          df.to_csv(file_full_path)
      