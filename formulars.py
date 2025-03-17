import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
import api_info as api
import warnings
import ta
import calendar
import requests
import json
from tqdm.auto import tqdm

from ta import add_all_ta_features
from ta.trend import EMAIndicator as ema

from datetime import timedelta, date, datetime

def get_all_stock_prices():
    database = pd.DataFrame()
    for i in tqdm(range(10, 24), leave = False):
        for j in tqdm(range(1, 13), leave = False):
            if j < 10:
                start_date = '20{}-0{}-01'.format(i,j)
                end_date = date(datetime.strptime(start_date, '%Y-%m-%d').year, datetime.strptime(start_date, '%Y-%m-%d').month, calendar.monthrange(datetime.strptime(start_date, '%Y-%m-%d').year, datetime.strptime(start_date, '%Y-%m-%d').month)[1]).strftime('%Y-%m-%d')
            else:
                start_date = '20{}-{}-01'.format(i,j)
                end_date = date(datetime.strptime(start_date, '%Y-%m-%d').year, datetime.strptime(start_date, '%Y-%m-%d').month, calendar.monthrange(datetime.strptime(start_date, '%Y-%m-%d').year, datetime.strptime(start_date, '%Y-%m-%d').month)[1]).strftime('%Y-%m-%d')
            floor = 'HOSE'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
            }
            url = 'https://finfo-api.vndirect.com.vn/v4/stock_prices/'
            query = '~date:gte:' + start_date + '~date:lte:' + end_date + '~floor:' + floor
            params = {'sort': 'date',
                     'size': 1000000,
                     'page': 1,
                     'q': query}
            response = requests.get(url, headers = headers, params = params)
            response
            data = response.text
            test= json.loads(data)
            a = pd.DataFrame(test['data'])
            if a.empty == True:
                continue
            else:
                database = pd.concat([database, a], axis = 0)
    return database

def get_all_tickers(database):
    tickers_all_ = [x for x in database.code.drop_duplicates()]
    all_tickers = []
    for i in tqdm(range(len(tickers_all_))):
        if len(tickers_all_[i]) == 3:
            all_tickers.append(tickers_all_[i])
    return all_tickers

def get_all_tickers_prices(database, all_tickers):
    '''
    Get all adjusted open, high, low, close, and transaction value
    '''
    dict = {}
    for i in tqdm(all_tickers):
        dict[i] = database[database['code']==i][['date', 'adOpen', 'adHigh', 'adLow', 'adClose', 'nmValue']]
        dict[i] = dict[i].set_index('date')
        dict[i].index = pd.to_datetime(dict[i].index)
        dict[i] = dict[i].sort_index()
    return dict

def get_df_all_tickers(dict, all_tickers, data_type = 'adClose'):
    '''Get a DataFrame of all tickers with 4 options: adOpen, adHigh, adLow, adClose, nmValue'''
    raw_prices = pd.DataFrame()
    for i in all_tickers:
        temp = dict[i][data_type]
        raw_prices = pd.concat([raw_prices, temp], axis = 1)
    raw_prices.columns = all_tickers
    return raw_prices

def get_vn_index():
    vni = pd.read_csv('vnindex_full.csv', index_col = 0, parse_dates=True).sort_index()
    vni.index = pd.to_datetime(vni.index)
    vni_add = api.get_index_values('VNINDEX', start_date = '2022-04-29')[['date', 'close']]
    vni_add.set_index('date', inplace = True)
    vni_add.columns = ['Close']
    vni_add.index = pd.to_datetime(vni_add.index)
    vni_add.sort_index(inplace =True)
    vni = pd.concat([vni, vni_add], axis = 0)
    return vni

def split_vni_train_test_real():
    train = vni['2015': '2019'].squeeze()
    test = vni['2020':'2021'].squeeze()
    real_time = vni['2022':].squeeze()
    return (train, test, real_time)

def date_range(start, end):
    delta = end-start # as timedelta
    days = [(start+timedelta(days = i)).strftime(format = '%Y-%m-%d') for i in range(delta.days + 1)]
    return days

def compare_two_series(dataframe, con_df, signal_name):
    pct_higher = (dataframe > con_df).sum(axis = 1)/dataframe.count(axis = 1)
    pct_higher = pct_higher.rename(signal_name)
    pct_higher = pd.DataFrame(pct_higher)
    return pct_higher


def rets_max(Series):
    '''
    - Kiểm tra return max từ ngày thứ 3 (Kể từ sau ngày ra signal) so với ngày đầu tiên vào trạng thái
    '''
    re = Series[3:].values.max()/Series.iloc[1] - 1
    return re

def rets_min(Series):
    '''
    - Kiểm tra return max từ ngày thứ 3 (Kể từ sau ngày ra signal) so với ngày đầu tiên vào trạng thái
    '''
    re = Series[3:].values.min()/Series.iloc[1] - 1
    return re

def rets_max_after(Series, t):
    '''
    - Cho ra chuỗi return cao nhất sau 1 ngày ra tín hiệu
    - Return phải đạt điều kiện có thể realize (T+3)
    '''
    df = Series.rolling(window = t).apply(rets_max).shift(-t+1)
    df.columns = ['rets_max']
    return df

def rets_min_after(Series, t):
    '''
    - Cho ra chuỗi return thấp nhấp sau 1 ngày ra tín hiệu
    - Return phải đạt điều kiện có thể realize (T+3)
    '''
    df = Series.rolling(window = t).apply(rets_min).shift(-t+1)
    df.columns = ['rets_min']
    return df

def min_series(Series):
    a = Series.min()
    return a

def max_series(Series):
    a = Series.max()
    return a

def find_low_peak_high_peak(Series, window = 9):
    low = Series.rolling(window = window).apply(min_series).shift(-int(window/2))
    low.columns = ['min_in_%s_days' %window]
    return low
    
    
def count_higher_high(Series):
    '''
    Count the number of higher high in a Series
    '''
    hh = Series.cummax()
    count_hh = 0
    for i in range(len(hh)):
        try:
            if hh.iloc[i+1, 0] > hh.iloc[i, 0]:
                count_hh += 1
        except:
            break
    return count_hh

def count_lower_low(Series):
    '''
    Count the number of lower low in a Series
    '''
    ll = Series.cummin()
    count_ll = 0
    for i in range(len(ll)):
        try:
            if ll.iloc[i+1, 0] < ll.iloc[i, 0]:
                count_ll += 1
        except:
            break
    return count_ll

def rets(Series):
    '''
    time: Number of days after the first day in the Series
    '''
    re = Series.iloc[-1]/Series.iloc[0] - 1
    return re

def find_rolling_count(count_type, days, Series):
    num_days = []
    for i in range(len(Series)):
        if i + days > len(Series):
            break
        else:
            num = count_type(Series[i:i+days])
            num_days.append(num)
    df = pd.DataFrame({
        'num_days': num_days
    }, index = Series.index[days - 1:])
    return df

def get_embargo_k_fold(Series, n, m, o, replace = False):
    '''
    Series format: [['Close', 'Rets']]
    o: Number of fold
    From a Series, get a dataframe of n + m sample with 
    train: take random from the beginning to period m
    test: take random from the period m to the end of dataset
    '''
    import pandas as pd
    import random
    import datetime
    
    final = pd.DataFrame()
    for i in range(0, o):
        try:
            ran1 = random.randint(Series.index[n].year + 1, Series.index[-m].year - 1)
        except:
            print('Train time exceeds test time')
            continue
        year1 = datetime.datetime(ran1, 1,1)
        year2 = datetime.datetime(random.randint(ran1, Series.index[-1].year),1,1)
        df = Series.iloc[:, 1][:year1].sample(n = n, replace = replace, random_state = i)
        df2 = Series.iloc[:, 1][year1:].sample(n = m, replace = replace, random_state = i)
        list_1 = [i for i in df]
        list_2 = [i for i in df2]
        list_all = list_1 + list_2
        df3 = pd.DataFrame({
            'Ran {}'.format(n+m): list_all
        })
        final = pd.concat([final, df3], axis = 1)
    return final