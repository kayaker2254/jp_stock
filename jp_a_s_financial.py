from os.path import join, relpath
from glob import glob
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime,timedelta
import shelve
csvex = shelve.open('/Users/shin/stubby/csvex')
csvex_id = csvex['csvex_id']
csvex_pass = csvex['csvex_pass']
csvex.close()

os.system((
        'wget --http-user={0} \
        --http-passwd={1} \
        "https://csvex.com/kabu.plus/csv/japan-all-stock-financial-results/monthly/japan-all-stock-financial-results.csv" \
        -P /Users/shin/Downloads/ \
        -NP /Users/shin/Downloads/ \
        -N').format(csvex_id,csvex_pass))

os.system(
        'nkf \
        -w \
        --overwrite \
        /Users/shin/Downloads/japan-all-stock-financial-results.csv')

fi_dt = datetime.fromtimestamp(os.stat('/Users/shin/Downloads/japan-all-stock-financial-results.csv').st_mtime)
fi_get_time = fi_dt.strftime('%Y-%m-%d %H:%M:%S')

columns_fi = [
        'st_code','name','kessannki','kessannhappyoubi','uriagedaka','eigyourieki','keijyourieki',
        'toukirieki','sousisann','jikosihonn','sihonnkinn','yuurisihusai','jikosihonnhiritu','roe',
        'roa','hakkouzumikabusikisuu'
        ]

fi_df = pd.read_csv(
        '/Users/shin/Downloads/japan-all-stock-financial-results.csv',
        skiprows = 1,
        na_values=('-',''),
        names = columns_fi)

n = len(fi_df)
fi_df['fi_get_time']= [fi_get_time]*n
fi_df['fi_get_time'] = pd.to_datetime(fi_df['fi_get_time'])

engine = create_engine('postgresql://shinya@localhost:5432/stock')

fi_df.to_sql(\
        'jp_al_st_financial',
        engine,
        if_exists = 'append',
        index=False
        )

