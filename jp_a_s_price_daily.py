from os.path import join, relpath
from glob import glob
import pandas as pd
from sqlalchemy import create_engine
import os, sys

# path = '/Users/shin/Downloads/'
# csv_files = [relpath(x, path) for x in glob(join(path, '*'))]

import shelve
csvex = shelve.open('/Users/shin/stubby/csvex')
csvex_id = csvex['csvex_id']
csvex_pass = csvex['csvex_pass']
csvex.close()

os.system(('wget \
        --http-user={0} \
        --http-passwd={1} \
        "https://csvex.com/kabu.plus/csv/japan-all-stock-prices/daily/japan-all-stock-prices.csv" \
        -P /Users/shin/Downloads/ \
        -NP /Users/shin/Downloads/ \
        -N'\
        ).format(csvex_id,csvex_pass))

columns = ['st_code','name','market','industry','date','st_pr','dbr_y','dbr_per','ld_cl_pr','start',
        'high','low','vol','value','capi','low_lim','upp_lim']

os.system('nkf \
        -w \
        --overwrite \
        /Users/shin/Downloads/japan-all-stock-prices.csv')

csv_files = glob('/Users/shin/Downloads/japan-all-stock-price*.csv')
list = []

for f in csv_files:
    list.append(pd.read_csv(f, skiprows = 1, na_values=('-',''), names = columns ))
df = pd.concat(list)

df['date'] = pd.to_datetime(df['date'])

engine = create_engine('postgresql://shinya@localhost:5432/stock')

df.to_sql(\
        'jp_al_st_pr', \
        engine, \
        if_exists = 'append', \
        index=False)
