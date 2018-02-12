#ダウンロードした shareholding は 表記が「33.4％」の様な表記
#になっていてデータフレームにするにはCSVファイルを一旦Excelで
#開き、表示形式を 数値 に修正して読み込む必要があったので自動化
#してみた。2017/12/30(土)

# module 読み込み。
import pandas as pd
from sqlalchemy import create_engine
import os

#CSVファイルをダウンロード
os.system('\
        wget --http-user="trial" "--http-passwd="PW@20170129"\
        https://hesonogoma.com/stocks/download/csv/japan-all-stock-information/monthly/shareholding-ratio.csv"\
        -P /Users/shin/Downloads/ \
        -NP /Users/shin/Downloads/ \
        -N')

#csvファイルをダウンロードした日時をデータフレームに加える為の準備。
sh_dt=datetime.datetime.fromtimestamp(os.stat('/Users/shin/Downloads/shareholding-ratio.csv').st_mtime)
sh_get_time = sh_dt.strftime('%Y-%m-%d %H:%M:%S')

#ダウンロードしたファイルをUTF-8に変換。
os.system('nkf -w --overwrite /Users/shin/Downloads/shareholding-ratio.csv')

#カラム名を格納。
columns_sh = ['st_code','hudou_hi','syousuu','tousisinntaku','gaikokujinn']

#ファイルを sh_df に読み込む。
sh_df = pd.read_csv('/Users/shin/Downloads/shareholding-ratio.csv', \
        skiprows = 1, \
        na_values=('-',''), \
        names = columns_sh)

#「%」を除去
sh_df.hudou_hi=(sh_df.hudou_hi.str.split('%',expand=True))
sh_df.syousuu=(sh_df.syousuu.str.split('%',expand=True))
sh_df.tousisinntaku=(sh_df.tousisinntaku.str.split('%',expand=True))
sh_df.gaikokujinn=(sh_df.gaikokujinn.str.split('%',expand=True))

#objectのままなので欠損値は0で穴埋め、数字部分を float に変換し、100で割る。
sh_df.hudouhi = ((sh_df[['hudou_hi']].fillna(0.0).astype(float))/100)
sh_df.syousuu=((sh_df[['syousuu']] .fillna(0.0).astype(float))/100)
sh_df.tousisinntaku=((sh_df[['tousisinntaku']] .fillna(0.0).astype(float))/100)
sh_df.gaikokujinn=((sh_df[['gaikokujinn']] .fillna(0.0).astype(float)/100))

#将来、持ち株比率が変化する事を考慮し、データを追加する為
#いつのデータなのかわかる様に日付カラムを追加する。
n=len(sh_df)
sh_df['get_time']=[sh_get_time]*n
sh_df['get_time'] = pd.to_datetime(sh_df['get_time'])

#PostgreSQLに接続する。
engine = create_engine('postgresql://shinya@localhost:5432/stock')

#既存の shareholding に append する。
sh_df.to_sql('shareholding', engine, if_exists = 'append', index=False)

