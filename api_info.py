import numpy as np
import pandas as pd
import requests
import json
import time
import os
import statsmodels.api as sm

from datetime import datetime
from datetime import date
from datetime import timedelta
from dateutil.relativedelta import relativedelta


def get_index_values(index_name, start_date, end_date=date.today().strftime('%Y-%m-%d'), size = 20000):
    '''From API, get index values including: 
    code, floor, date, time, type, open, high, low, close, change, pctChange, accumulatedVol, accumulatedVal, nmVolume, nmValue, 
    ptVolume, ptValue, advances, declines, noChange, noTrade, ceilingStocks, floorStocks
    index_name: 
    0         VNINDEX
    1           VNSML
    2            VN30
    3           VNMID
    4           VN100
    5          VNREAL
    6           VNX50
    7           HNX30
    8       VNFINLEAD
    9             HNX
    10          VNALL
    11           VNSI
    12          VNENE
    13           VNIT
    14          VNMAT
    15          VNUTI
    16         VNHEAL
    17      VNDIAMOND
    18          VNIND
    19    VNFINSELECT
    20         VNCONS
    21          VNFIN
    22         VNCOND
    23         VNXALL
    24          UPCOM
    start_date & end_date format: %yyyy-mm-dd'''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    url = 'https://finfo-api.vndirect.com.vn/v4/vnmarket_prices'
    query = '~date:gte:' + start_date + '~date:lte:' + end_date + '~code:' + index_name
    params = {'sort': 'date',
             'size': size,
             'page': 1,
             'q': query}
    response = requests.get(url, headers = headers, params = params)
    print(response)
    data = response.text
    test= json.loads(data)
    a = pd.DataFrame(test['data'])
    return a

def get_stock_data(ticker_name, start_date, end_date=date.today().strftime('%Y-%m-%d'), size=10000):
    '''
    Get stock price data including:
    'code', 'date', 'time', 'floor', 'type', 'basicPrice', 'ceilingPrice',
    'floorPrice', 'open', 'high', 'low', 'close', 'average', 'adOpen',
    'adHigh', 'adLow', 'adClose', 'adAverage', 'nmVolume', 'nmValue',
    'ptVolume', 'ptValue', 'change', 'adChange', 'pctChange'
    '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    url = 'https://finfo-api.vndirect.com.vn/v4/stock_prices'
    query = 'code:' + ticker_name + '~date:gte:' + start_date + '~date:lte:' + end_date
    params = {'sort': 'date',
             'size': size,
             'page': 1,
             'q': query}
    response = requests.get(url, headers = headers, params = params)
    response
    data = response.text
    test= json.loads(data)
    a = pd.DataFrame(test['data'])
    return a

def get_supply_demand(ticker_name, start_date, end_date=date.today().strftime('%Y-%m-%d'), size = 10000):
    '''
    Get supply and demand data including:
    'type', 'code', 'lastVol', 'accumulatedVol', 'lastPrice', 'tradingDate',
    'time', 'bidPrice01', 'bidQtty01', 'bidPrice02', 'bidQtty02',
    'bidPrice03', 'bidQtty03', 'bidPrice04', 'bidQtty04', 'bidPrice05',
    'bidQtty05', 'bidPrice06', 'bidQtty06', 'bidPrice07', 'bidQtty07',
    'bidPrice08', 'bidQtty08', 'bidPrice09', 'bidQtty09', 'bidPrice10',
    'bidQtty10', 'offerPrice01', 'offerQtty01', 'offerPrice02',
    'offerQtty02', 'offerPrice03', 'offerQtty03', 'offerPrice04',
    'offerQtty04', 'offerPrice05', 'offerQtty05', 'offerPrice06',
    'offerQtty06', 'offerPrice07', 'offerQtty07', 'offerPrice08',
    'offerQtty08', 'offerPrice09', 'offerQtty09', 'offerPrice10',
    'offerQtty10'
    '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    url = 'https://finfo-api.vndirect.com.vn/v4/supply_demands'
    query = '~code:' + ticker_name + '~tradingDate:gte:' + start_date + '~tradingDate:lte:' + end_date 
    params = {'page': 1,
              'sort': 'tradingDate',
              'size': 1000,
              'q': query}
    response = requests.get(url, headers = headers, params = params)
    print(response)
    data = response.text
    test= json.loads(data)
    a = pd.DataFrame(test['data'])
    return a.replace(0, np.nan).dropna(axis = 1)

def get_financial_info(ticker, report_type='QUARTER'):
    '''
    From API, get all financial information
    '''
    url = 'https://finfo-api.vndirect.com.vn/v4/financial_statements'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'}
    report_type= 'QUARTER'
    query = 'size=20000' + '&sort=fiscalDate' + '&q=code:'+ ticker + '~reportType:' + report_type
    response_1 = requests.get(url, headers=headers, params=query)
    response_1
    data_1 = response_1.text
    test_1= json.loads(data_1)
    a_1 = pd.DataFrame(test_1['data'])
    a_1
    database_2 = a_1[['itemCode','numericValue','fiscalDate']]
    database_2
    index = ['itemCode','fiscalDate']
    database_2 = database_2.set_index(index)
    return database_2

def get_financial_value(series, ticker):
    '''
    Get itemcode value from financial information
    '''
    database_2 = get_financial_info(ticker)
    dict = {}
    for itemCode in series:
        dict['%s'%itemCode] = database_2.query('itemCode==@itemCode').droplevel(0).sort_index().loc['2012':].dropna()
        dict['%s'%itemCode].index = pd.to_datetime(dict['%s'%itemCode].index)
        dict['%s'%itemCode].index = dict['%s'%itemCode].index+pd.Timedelta(1, unit='D')
        dict['%s'%itemCode].index = dict['%s'%itemCode].index - pd.Timedelta(90,unit='D')
        dict['%s'%itemCode].index = pd.to_datetime(pd.DataFrame({
            'day':1,
            'month': dict['%s'%itemCode].index.month,
            'year': dict['%s'%itemCode].index.year
        }))
    return dict
    
def get_financial_models(ticker, model_type, size=10000):
    '''
    From API, get financial statements of different companies
    model_type:
        1. Balancesheet
        2. Income
        3. Cashflow
        4. Explaination
        5. Estimation
        6. Highlight
    '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    url = 'https://finfo-api.vndirect.com.vn/v4/financial_models'
    query = '~codeList:' + ticker + '~modelTypeName:' + model_type
    params = {'sort': 'modelTypeName',
                 'size': size,
                 'page': 1,
                 'q': query}
    response = requests.get(url, headers = headers, params = params)
    print(response)
    data = response.text
    test= json.loads(data)
    a = pd.DataFrame(test['data'])
    return a

def get_financial_ratios(ticker, ratio_code, start_date, end_date = date.today().strftime('%Y-%m-%d')):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    url = 'https://finfo-api.vndirect.com.vn/v4/ratios'
    query = 'code:' + ticker + '~reportDate:gte:' + start_date + '~ratioCode:' + ratio_code + '~reportDate:lte:' + end_date
    params = {'size': '10000',
             'sort': 'reportDate',
             'page': 1,
             'q': query}
    response = requests.get(url, headers=headers, params = params)
    data = response.text
    test = json.loads(data)
    a = pd.DataFrame(test['data'])
    return a

def get_derivatives_price(deriCode, start_date, end_date = date.today().strftime('%Y-%m-%d')):
    '''
    Lấy giá sản phẩm phái sinh và giá chứng quyền.
    - deriCode:
    'GB05F1Q',
    'GB10F1Q',
    'GB10F3Q',
    'VN30F2M',
    'GB10F2Q',
    'GB05F2Q',
    'VN30F1M',
    'GB05F3Q',
    'VN30F2Q',
    'VN30F1Q'
    '''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    url = 'https://finfo-api.vndirect.com.vn/v4/derivative_prices'
    query = '~date:gte:' + start_date + '~date:lte:' + end_date + '~deriCode:' + deriCode
    params = {'size': '20000',
             'sort': 'date',
             'q': query}
    response = requests.get(url, headers = headers, params = params)
    data = response.text
    test = json.loads(data)
    a = pd.DataFrame(test['data'])
#     a = a[['date', 'close']].set_index('date')
#     a.index = pd.to_datetime(a.index)
#     a.sort_index(inplace=True)
    return a

def get_cw_price(type, start_date, end_date = date.today().strftime('%Y-%m-%d'), **kwargs,):
    '''
    *kwargs = code
    '''
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    url = 'https://finfo-api.vndirect.com.vn/v4/derivative_prices'
    query = 'type:' + type + '~date:gte:' + start_date + '~date:lte:' + end_date
    params = {'size': '50000',
             'sort': 'date',
             'q': query}
    response = requests.get(url, headers = headers, params = params)
    data = response.text
    test = json.loads(data)
    a = pd.DataFrame(test['data'])
    a = a.set_index('date')
    a.index = pd.to_datetime(a.index)
    a.sort_index(inplace=True)
    return a

def get_foreign_investors_position(ticker, start_date = '2022-01-01', end_date = date.today().strftime('%Y-%m-%d'), size = 20000):
    '''
    ticker : Tên cổ phiếu, tên ETF
    '''
    url = 'https://finfo-api.vndirect.com.vn/v4/foreigns'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'}
    query = '~code:' + ticker, '~tradingDate:gte:' + start_date + '~tradingDate:lte:' + end_date
    params = {'sort': 'tradingDate',
             'size': size,
             'page': 1,
             'q': query}
    response = requests.get(url, headers = headers, params = params)
    data = response.text
    test = json.loads(data)
    a = pd.DataFrame(test['data'])
    return a

def get_proprietary_trading(ticker, start_date, end_date):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    url = 'https://finfo-api.vndirect.com.vn/v4/proprietary_trading'
    query = '~date:gte:' + start_date + '~date:lte:' + end_date
    params = {'sort': 'date',
             'size': 100000,
             'page': 1,
             'q': query}
    response = requests.get(url, headers = headers, params = params)
    print(response)
    data = response.text
    test= json.loads(data)
    a = pd.DataFrame(test['data'])
    return a