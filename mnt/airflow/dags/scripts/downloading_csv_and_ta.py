from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time

import os.path


# https://technical-analysis-library-in-python.readthedocs.io/en/latest/index.html
# technical analysis library
import pandas as pd
from ta import add_all_ta_features
from ta.utils import dropna

import os
import shutil

from yahoo_fin.stock_info import get_data


def remove_files(directory='data/'):
    for root, dirs, files in os.walk(directory):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))
            
def add_ta_and_save_new_csv(portfolio, date_1, date_2,folder_to_save='data/',time_interval='1d'):
    remove_files()
    for stock in portfolio:
        df = get_data(stock, start_date=date_1, end_date=date_2, index_as_date = True, interval=time_interval)
#         df = pd.read_csv(url)
        # period1=unixtime, , Sun Jun 26 2022 00:00:00 GMT+0000, Mon Sep 26 2022 00:00:00 GMT+0000 
        # https://www.unixtimestamp.com/
#         print(df)

        # Clean NaN values
        df = dropna(df)

        # Add ta features filling NaN values
        df = add_all_ta_features(
            df, open="open", high="high", low="low", close="adjclose", volume="volume", fillna=True)
        df.to_csv(path_or_buf=f'{folder_to_save}/{stock}.csv',index=False)
        time.sleep(2)

path = '/data'
isExist = os.path.exists(path)
    
stock_portfolio =[]
with open('portfolio.txt',newline= '') as f:
    lines = f.readlines()
    for i in range(len(lines)):
        stock_portfolio.append(lines[i].strip('\n'))

date_2 = date.today()

# print(datetime.combine(date_1, datetime.min.time()))
date_1 = date.today() + relativedelta(months=-3)
# date_2 = str(date_1).replace('-','/')

# date_1 = str(date_1).replace('-','/')

file_exists = os.path.exists('readme.txt')

add_ta_and_save_new_csv(portfolio=stock_portfolio, date_1=date_1, date_2=date_2)



print(stock_portfolio)

