# coding: utf-8
#必要そうなmoduleを乗せとく。
import sys, os
import datetime
from glob import glob
from os.path import join, relpath
import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
from sqlalchemy import create_engine
import pandas.tseries.offsets as offsets
import numpy as np
import matplotlib.pyplot as plt

#/Users/shin/Downloads/ 内の data_daily ファイルをフルパスで取得。
csv_files_path = glob('/Users/shin/Downloads/japan-all-stock-dat*')

#/Users/shin/Downloads/ 内の data_daiky ファイルのファイル名だけ取得。
path = '/Users/shin/Downloads/'
csv_files_name = [relpath(x, path) for x in glob(join(path, 'japan-all-stock-data_*'))]

#nkf で UTF-8 に変換。
os.system('nkf -w --overwrite /Users/shin/Downloads/japan-all-stock-data_*.csv')

#日付入りのファイルネームから日付箇所を抜き出す。
date_name_list =[]
for list in range(len(csv_files_name)):
　　name = csv_files_name[list][21:-4]
    date_name_list.append(name)

print(date_name_list)
