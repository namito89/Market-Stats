import pandas as pd
import requests
import json
import datetime
import time
import os
import numpy as np
import pandas as pd
import scipy.stats as lm
import matplotlib as plot
import statsmodels.api as sm
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from datetime import date
import api_info as api
from tqdm import tqdm

def get_financial_info(ticker):
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
    
def get_item_code_value(r, ticker):
    'lấy dữ liệu từ database_2'
    database_2 = get_financial_info(ticker)
    query = 'itemCode==r'.replace('r',r)
    x = database_2.query(query)
    index = x.index.get_level_values(2)
    y = x.squeeze().values
    y = pd.DataFrame(y, index)
    y = y.sort_index()
    y.index = pd.to_datetime(y.index)
    y = y.squeeze()
    return y

from statsmodels.tsa.stattools import adfuller
def stationary_test(Series):
    result = adfuller(Series)
    print('ADF Statistic: %f' % result[0])
    print('p-value: %f' % result[1])
    print('Critical Values:')
    for key, value in result[4].items():
        print('\t%s: %.3f' % (key, value))
    
def highlight_cols(x):
    # copy df to new - original data is not changed
    df = x.copy()
    # select all values to green color
    df.loc[:, :] = 'background-color: white'
    # overwrite values grey color
    df.iloc[:,2::3] = 'background-color: green'
    # return color df
    return df

def pro_b_0(r):
    test_corr_in_sample = excess.squeeze()*fitted
    test_corr_test = excess.squeeze()*predicted
    pos_count, neg_count = 0,0
    for i in r:
        if i>=0:
            pos_count += 1
        else:
            neg_count += 1
    p_win_in_sample = pos_count/len(r.dropna())
    print('Xắc suất phán đoán đúng của mô hình: ', p_win_in_sample)
    return p_win_in_sample


def get_financial_models(ticker):
    url = 'https://finfo-api.vndirect.com.vn/v4/financial_models'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    modelType = 'income'
    query = 'size=1000' + '&q=codeList:'+ ticker + '~modelTypeName:'+ modelType
    params = {'q': query}
    response = requests.get(url, headers=headers, params=query)
    data = response.text
    test= json.loads(data)
    income_statement = pd.DataFrame(test['data'])
    modelType = 'balancesheet'
    query = 'size=1000' + '&q=codeList:'+ ticker + '~modelTypeName:'+ modelType
    params = {'q': query}
    response = requests.get(url, headers=headers, params=query)
    data = response.text
    test= json.loads(data)
    balance_sheet = pd.DataFrame(test['data'])
    balance_sheet.head()
    modelType = 'cashflow'
    query = 'size=1000' + '&q=codeList:'+ ticker + '~modelTypeName:'+ modelType
    params = {'q': query}
    response = requests.get(url, headers=headers, params=query)
    data = response.text
    test= json.loads(data)
    cash_flow = pd.DataFrame(test['data'])
    bctc_all = pd.concat([cash_flow, balance_sheet, income_statement], axis = 0)
    series = bctc_all['itemCode'].drop_duplicates(keep='first', inplace=False)
    return income_statement, balance_sheet, cash_flow, bctc_all, series

def series_itemCode_income(ticker):
    url = 'https://finfo-api.vndirect.com.vn/v4/financial_models'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    modelType = 'income'
    query = 'size=1000' + '&q=codeList:'+ ticker + '~modelTypeName:'+ modelType
    params = {'q': query}
    response = requests.get(url, headers=headers, params=query)
    data = response.text
    test= json.loads(data)
    income_statement = pd.DataFrame(test['data'])
    series_a1 = income_statement['itemCode'].drop_duplicates(keep = 'first')
    return series_a1

def series_itemCode_balancesheet(ticker):
    url = 'https://finfo-api.vndirect.com.vn/v4/financial_models'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    modelType = 'balancesheet'
    query = 'size=1000' + '&q=codeList:'+ ticker + '~modelTypeName:'+ modelType
    params = {'q': query}
    response = requests.get(url, headers=headers, params=query)
    data = response.text
    test= json.loads(data)
    income_statement = pd.DataFrame(test['data'])
    series_b1 = balance_sheet['itemCode'].drop_duplicates(keep = 'first')
    return series_b1
    
    
def series_itemCode_cashflow(ticker):
    url = 'https://finfo-api.vndirect.com.vn/v4/financial_models'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    modelType = 'cashflow'
    query = 'size=1000' + '&q=codeList:'+ ticker + '~modelTypeName:'+ modelType
    params = {'q': query}
    response = requests.get(url, headers=headers, params=query)
    data = response.text
    test= json.loads(data)
    income_statement = pd.DataFrame(test['data'])
    series_c1 = income_statement['itemCode'].drop_duplicates(keep = 'first')
    return series_c1



def get_financial_info_per_itemCodes(series, ticker):
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
        dict['%s'%itemCode].index = [x + relativedelta(months = 4) for x in dict['%s'%itemCode].index]
        dict['%s'%itemCode] = dict['%s'%itemCode].replace(np.inf, np.nan).dropna()
    return dict

def get_itemVnName(itemCodes, bctc_all):
    vn_dataframe = pd.DataFrame()
    for itemCode in itemCodes:
        vn = pd.DataFrame({
            'itemVnName': bctc_all.set_index('itemCode').loc[itemCode, 'itemVnName'].drop_duplicates()
        })
        vn_dataframe = pd.concat([vn_dataframe, vn], axis = 0)
    return vn_dataframe

def get_financial_ratios(itemCodes, ticker):
    income_statement, balance_sheet, cash_flow, bctc_all, series = get_financial_models(ticker)
    dict = get_financial_info_per_itemCodes(series, ticker)
    itemCode_1_a = itemCodes[0] 
    itemCode_2_a = itemCodes[1] 
    itemCode_3_a = itemCodes[2] 
    itemCode_4_a = itemCodes[3] 
    itemCode_5_a = itemCodes[4] 
    itemCode_6_a = itemCodes[5]
    if len(itemCodes) >= 7:
        itemCode_7_a = itemCodes[6]
    if len(itemCodes) >= 8:
        itemCode_7_a = itemCodes[7]
    ratio_1 = dict['%s'%itemCode_1_a]/dict['%s'%itemCode_2_a]
    ratio_2 = dict['%s'%itemCode_3_a]/dict['%s'%itemCode_4_a]
    ratio_3 = dict['%s'%itemCode_5_a]/dict['%s'%itemCode_6_a]
    if len(itemCodes) >= 8:
        ratio_4 = dict['%s'%itemCode_5_a]/dict['%s'%itemCode_6_a]
    return ratio_1, ratio_2, ratio_3

def compound(r):
    '''
    Returns the result of compounding the set of returns in r
    '''
    return np.expm1(np.log1p(r).sum())


def get_excess_rets_daily(ticker):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
    }
    url = 'https://finfo-api.vndirect.com.vn/v4/stock_prices/'
    start_date = '2010-01-01'
    today = date.today().strftime('%Y-%m-%d')
    query = 'code:' + ticker + '~date:gte:' + start_date + '~date:lte:' + today
    params = {'sort': 'date',
             'size': 20000,
             'page': 1,
             'q': query}
    response = requests.get(url, headers = headers, params = params)
    response
    data = response.text
    test= json.loads(data)
    sample_1 = pd.DataFrame(test['data'])
    #sample_2 : Select date and adjusted close price columns
    sample_2 = sample_1[['date', 'adClose']].set_index('date').sort_index()
    sample_2.index = pd.to_datetime(sample_2.index)
    excess = sample_2.pct_change() - 0.07/365
    return excess.dropna()


def get_excess_rets_quarterly(ticker):
    sample_2 = api.get_stock_data(ticker, start_date = '2010-01-01')[['date', 'adClose']].set_index('date').sort_index()
    sample_2.index = pd.to_datetime(sample_2.index)
    excess = sample_2.pct_change() - 0.07/365
    sample_3 = (sample_2.groupby(pd.Grouper(freq = 'Q-JAN', closed = 'right')).last().pct_change() - 0.07/4)
    sample_3.index = sample_3.index + timedelta(days = 1)
    excess = sample_3.shift(-1)
    excess = excess['2010-01-01':]
    return excess

def get_regression_dataframe_1var_ind(ItemCode_Series, ticker, n_periods = 4):
    # dict: dictionary of each item code series
    dict = {}
    dataframe_m1 = pd.DataFrame()
    dataframe_n1 = pd.DataFrame()
    database_2 =  get_financial_info(ticker)
    excess = get_excess_rets_quarterly(ticker)
    income_statement, balance_sheet, cash_flow, bctc_all, series = get_financial_models(ticker)
    for itemCode in ItemCode_Series:
        # Shifting to the first day of the quarter
        dict['%s'%itemCode] = database_2.query('itemCode==@itemCode').droplevel(0).sort_index().loc['2012':'2019'].pct_change(periods = n_periods).dropna()
        dict['%s'%itemCode].index = pd.to_datetime(dict['%s'%itemCode].index)
        dict['%s'%itemCode].index = dict['%s'%itemCode].index+pd.Timedelta(1, unit='D')
        dict['%s'%itemCode].index = dict['%s'%itemCode].index - pd.Timedelta(90,unit='D')
        dict['%s'%itemCode].index = pd.to_datetime(pd.DataFrame({
            'day':1,
            'month': dict['%s'%itemCode].index.month,
            'year': dict['%s'%itemCode].index.year
        }))
        dict['%s'%itemCode].index = [x + relativedelta(months = 4) for x in dict['%s'%itemCode].index]
        dict['%s'%itemCode] = dict['%s'%itemCode].replace(np.inf, np.nan).dropna()
        if len(dict['%s'%itemCode].sort_index().loc['2014-01-01':'2018-10-01'].replace(np.inf, np.nan).replace(-np.inf, np.nan).dropna()) != 19:
            continue
        if type(bctc_all.set_index('itemCode').loc[itemCode, 'itemVnName']) == str:
            data_a = bctc_all.set_index('itemCode').loc[itemCode, 'itemVnName']
        else:
            data_a = bctc_all.set_index('itemCode').loc[itemCode, 'itemVnName'].drop_duplicates().values[0]
        dict['%s'%itemCode].loc[:,'Constant']=1
        lm = sm.OLS(excess['2014-01-01':'2018-10-01'], dict['%s'%itemCode].sort_index().loc['2014-01-01':'2018-10-01']).fit()
        dataframe_n1 = pd.DataFrame({
        'itemCode': itemCode,
        'itemCode_Vn': data_a,
        'Coef 1': [lm.params[0]],
        'Coef 1 std_err': [lm.bse[0]],
        'Coef 1_pv': [lm.pvalues[0]],
        'Constant': [lm.params[1]],
        'Constant std_err': [lm.bse[1]],
        'Constant_pv': [lm.pvalues[1]],
        'AIC': [lm.aic],
        'BIC': [lm.bic],
        'R-squared': [lm.rsquared],
        })
        dataframe_m1 = pd.concat([dataframe_m1, dataframe_n1], axis = 0, join='outer')
    return dataframe_m1

def get_csv_file(dataframe, ticker, var_type):
    '''
    Save to CSV file in BCTC_regress\ticker folder
    
    dataframe: dataframe to convert to csv file
    ticker: name of the company
    var_type: Financial indicators or ratios
    modelType: YoY or QoQ or TTM
    '''
    newpath = rf'D:\Quantamental_Model\BCTC_Regression\{ticker}'
    if not os.path.exists(newpath):
        os.makedirs(newpath)
    if dataframe.empty:
        return
    else:
        dataframe.sort_values(by='R-squared', ascending=False).to_excel(newpath + rf'\{ticker}_{var_type}.xlsx', index = False)
    

def get_regression_dataframe_ratio(ItemCode_series, ticker, n_periods = 4):
    dict = {}
    dataframe_m1 = pd.DataFrame()
    dataframe_n1 = pd.DataFrame()
    database_2 =  get_financial_info(ticker)
    excess = get_excess_rets_quarterly(ticker)
    income_statement, balance_sheet, cash_flow, bctc_all, series = get_financial_models(ticker)
    for itemCode in ItemCode_series:
        # Shifting to the first day of the quarter
        dict['%s'%itemCode] = database_2.query('itemCode==@itemCode').droplevel(0).sort_index().loc['2012':'2019'].pct_change(periods = n_periods).dropna()
        dict['%s'%itemCode].index = pd.to_datetime(dict['%s'%itemCode].index)
        dict['%s'%itemCode].index = dict['%s'%itemCode].index+pd.Timedelta(1, unit='D')
        dict['%s'%itemCode].index = dict['%s'%itemCode].index - pd.Timedelta(90,unit='D')
        dict['%s'%itemCode].index = pd.to_datetime(pd.DataFrame({
            'day':1,
            'month': dict['%s'%itemCode].index.month,
            'year': dict['%s'%itemCode].index.year
        }))
        dict['%s'%itemCode].index = [x + relativedelta(months = 4) for x in dict['%s'%itemCode].index]
        dict['%s'%itemCode] = dict['%s'%itemCode].replace(np.inf, np.nan).dropna()
    indicators = []
    for i in ItemCode_series:
        for j in ItemCode_series:
            if i == j:
                continue
            indicators.append([i,j])
    dataframe_m2 = pd.DataFrame()
    for itemCode_1, itemCode_2 in tqdm(indicators, desc='inner loop', position = 0, leave = True):
        m = dict[str(itemCode_1)]['2012':'2019']
        n = dict[str(itemCode_2)]['2012':'2019']
        m = m.replace(0, np.nan).dropna()
        n = n.replace(0, np.nan).dropna()
        if len(m) != len(n):
            continue
        if len(m) == 0 or len(n) == 0:
            continue
        financial_ratios = (m/n).pct_change(periods = n_periods).dropna()
        if len(financial_ratios['2014-08-01':'2018-10-01']) != len(excess['2014-08-01':'2018-10-01']):
            continue
        if len(financial_ratios['2014-08-01':'2018-10-01'].dropna()) != 17:
            continue
        if np.nan in financial_ratios['2014-08-01':'2018-10-01'].dropna():
            continue
        if -np.nan in financial_ratios['2014-08-01':'2018-10-01'].dropna():
            continue
        
        financial_ratios['Constant'] = 1
        if type(bctc_all.set_index('itemCode').loc[itemCode_1, 'itemVnName']) == str:
            data_a = bctc_all.set_index('itemCode').loc[itemCode_1, 'itemVnName']
        else:
            data_a = bctc_all.set_index('itemCode').loc[itemCode_1, 'itemVnName'].drop_duplicates().values[0]
        if type(bctc_all.set_index('itemCode').loc[itemCode_2, 'itemVnName']) == str:
            data_b = bctc_all.set_index('itemCode').loc[itemCode_2, 'itemVnName']
        else:
            data_b = bctc_all.set_index('itemCode').loc[itemCode_2, 'itemVnName'].drop_duplicates().values[0]
        try:
            lm = sm.OLS(excess['2014-08-01':'2018-10-01'], financial_ratios['2014-08-01':'2018-10-01']).fit()
        except:
            continue
        temp_df = pd.DataFrame({
            'itemCode_1': itemCode_1,
            'itemCode_1_Vn': [data_a],
            'itemCode_2': itemCode_2,
            'itemCode_2_Vn': [data_b],
            'Coef 1': [lm.params[0]],
            'Coef 1 std_err': [lm.bse[0]],
            'Coef 1_pv': [lm.pvalues[0]],
            'Constant': [lm.params[1]],
            'Constant std_err': [lm.bse[1]],
            'Constant_pv': [lm.pvalues[1]],
            'AIC': [lm.aic],
            'BIC': [lm.bic],
            'R-squared': [lm.rsquared],
            })
        dataframe_m2 = pd.concat([dataframe_m2, temp_df], axis = 0, join='outer')
    return dataframe_m2

def get_combination_3_vars(series):
    itemCode_list = []
    for i in series:
        for j in series:
            for k in series:
                if i == j:
                    continue
                if j == k:
                    continue
                if k == i:
                    continue
            itemCode_list.append([i,j,k])
    return itemCode_list
