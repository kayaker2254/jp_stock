#from os.path import join, relpath
from glob import glob
import pandas as pd
from sqlalchemy import create_engine

#path = '/Users/shin/Downloads/'
#csv_files = [relpath(x, path) for x in glob(join(path, '*'))]

csv_files = glob('/Users/shin/Downloads/*.csv')
list = []

for f in csv_files:
    list.append(pd.read_csv(f))
df = pd.concat(list)

engine = create_engine('postgresql://shinya@localhost:5432/stok')

df.to_sql('jpn_all_financial_results_20171201', engine, index=False)
