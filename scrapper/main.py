import datetime

from nikescrapi import NikeScrAPI
from sales_generator import SalesGenerator

nikeAPI = NikeScrAPI(max_pages=1, path='data/products')
df = nikeAPI.getData()

# Sales generator
min_sales=0 # minimum tickets for each day sales
max_sales=5 # maximum tickets for each day sales
day_count = 90 # generate sales for n previous days use 0 for today only

gen = SalesGenerator(nike_df=df, min_sales=min_sales, max_sales=max_sales)

end = datetime.datetime.now()
start = end - datetime.timedelta(days=day_count)
gen.generate_interval(start=start, end=end)
