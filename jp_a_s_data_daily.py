from os.path import join, relpath
from glob import glob
import pandas as pd
from sqlalchemy import create_engine
import os, sys
from datetime import datetime,timedelta
sys.path.append('/Users/shin/stubby')
from csvex import *

os.system(('wget\
        --http-user={0}\
        --http-passwd={1}\
        "https://csvex.com/kabu.plus/csv/japan-all-stock-data/daily/japan-all-stock-data.csv" \
        -P /Users/shin/Downloads/ \
        -NP /Users/shin/Downloads/ \
        -N'\
        ) \
        .format(csvex_id,csvex_pass))

download_time = datetime.fromtimestamp(os.stat('/Users/shin/Downloads/japan-all-stock-data.csv').st_mtime)

get_date = download_time.strftime('%Y-%m-%d %H:%M:%S')

os.system('nkf \
        -w \
        --overwrite \
        /Users/shin/Downloads/japan-all-stock-data.csv'\
        )

csv_file = '/Users/shin/Downloads/japan-all-stock-data.csv'

jp_al_st__dd_colmns = [\
        'st_code', 'name', 'market', 'industry', 'jika_sougaku', 'hakkouzumi_kabusikisuu', \
        'haitou_rimawari', 'hitokabu_haitou', 'per_yosoku', 'pbr_jisseki', 'eps_yosoku', \
        'bps_jisseki', 'saitei_kounyuugaku', 'tanngennkabu', 'takane_hiduke', 'nennsyorai_takane', \
        'yasune_hiduke', 'nennsyorai_yasune'\
        ]

#1行目は指標名、2行目、3行目は株価指数の為、読み飛ばす。
data_daily = pd.read_csv(\
        csv_file, skiprows = 3, \
        na_values=('-',''), \
        names = jp_al_st__dd_colmns \
        )

data_daily['takane_hiduke'] = pd.to_datetime(data_daily['takane_hiduke'])
data_daily['yasune_hiduke'] = pd.to_datetime(data_daily['yasune_hiduke'])

n = len(data_daily)
data_daily['get_date'] = [get_date] * n
data_daily['get_date'] = pd.to_datetime(data_daily['get_date'])

engine = create_engine('postgresql://shinya@localhost:5432/stock')

data_daily.to_sql(
        'jp_al_st_data_daily', \
        engine, \
        if_exists = 'append', \
        index=False \
        )
