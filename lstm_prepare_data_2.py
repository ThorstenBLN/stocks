import yfinance as yf
import pandas as pd
import numpy as np
import time
import os
import openpyxl
import datetime as dt
import warnings
import functions as f
import re
import logging
import sys
import janitor
import matplotlib.pyplot as plt
import lstm_functions as f
import pickle

plt.rcParams['figure.dpi'] = 400

PATH_DATA = "./data_lstm/"
PATH_DATA_BASE = "./data/"
FILE_HIST = "prices_historic.csv"
FILE_CLASS =  "prices_class.csv"
FILE_FINAL = "data_lstm.csv"
FILE_FINAL_IND = "data_lstm_ind_2_classes_q175.csv"
FILE_INDICES = "prices_historic_indices.csv"
FILE_EXCLUDE = "exclude_isin.csv"
FIRST_YEAR = 2020
EPS = 10e-8

# 1. load all data
df_all = pd.read_csv(PATH_DATA + FILE_HIST).clean_names(strip_underscores=True)
df_all['date'] = df_all['date'].astype(str).str[:11]
df_all['date'] = pd.to_datetime(df_all['date'])
df_all = df_all.loc[df_all['date'].dt.year >= FIRST_YEAR][['data_date', 'symbol', 'isin', 
                                                           'date', 'close', 'volume', 'open', 
                                                           'high', 'low', 'dividends', 'stock_splits', 
                                                           'adj_close', 'capital_gains']].sort_values(['isin', 'date'])

# # 2. calculate the class for each timeframe (following 4 weeks development of price)
# # 2.1 calculate the individual classes
DAYS_FUTURE = 5
THRES_QTL = 0.20
df_all['close_future'] = df_all.groupby('isin')['close'].transform(lambda x: x.shift(-DAYS_FUTURE))
df_all['close_mean'] = df_all.groupby('isin').rolling(DAYS_FUTURE, min_periods=DAYS_FUTURE, center=False, closed='both')['close_future'].mean().values
df_all['target'] = np.log(df_all['close_mean']) - np.log(df_all['close']) 
print(df_all['target'].describe())
thres = (np.abs(df_all['target'].quantile(THRES_QTL)) + df_all['target'].quantile(1-THRES_QTL)) / 2
print(thres)
df_all['class'] = np.where(df_all['target'] > thres, 0, np.where(df_all['target'] < -thres, 1, 2))
df_all['class'].value_counts()
# --> class 0: clear up, class 1: clear down, class 2: flat (discard)
df_all.sort_values(['isin', 'date'], inplace=True)
df_all.to_csv(PATH_DATA + FILE_CLASS, index=False)

# 3. feature engineering
# calculate returns 
df_all['ret_1'] = df_all.groupby('isin')['close'].transform(lambda x: np.log(x) - np.log(x.shift(1)))
df_all['ret_5'] = df_all.groupby('isin')['close'].transform(lambda x: np.log(x) - np.log(x.shift(5)))
df_all['ret_20'] = df_all.groupby('isin')['close'].transform(lambda x: np.log(x) - np.log(x.shift(20)))
# calculate RSI
N = 14
df_all['u'] = df_all.groupby('isin')['close'].transform(lambda x: x - x.shift(1))
df_all['u'] = np.where(df_all['u'] < 0, 0, df_all['u'])
df_all['d'] = df_all.groupby('isin')['close'].transform(lambda x: x.shift(1) - x)
df_all['d'] = np.where(df_all['d'] < 0, 0, df_all['d'])
df_all['ewm_u'] = df_all.groupby(['isin'])['u'].ewm(alpha=1/N, ignore_na=True, min_periods=20).mean().values
df_all['ewm_d'] = df_all.groupby(['isin'])['d'].ewm(alpha=1/N, ignore_na=True, min_periods=20).mean().values
df_all['RSI'] = 1 - 1 / (1 + df_all['ewm_u'] / df_all['ewm_d'])
# calculate moving average ratios
df_all['ma_5'] = df_all.groupby('isin').rolling(5, min_periods=5, center=False, closed='left')['close'].mean().values # ma until x_t-1
df_all['ma_ratio_5'] = np.log(df_all['close']) - np.log(df_all['ma_5'])
df_all['ma_10'] = df_all.groupby('isin').rolling(10, min_periods=10, center=False, closed='left')['close'].mean().values # ma until x_t-1
df_all['ma_ratio_10'] = np.log(df_all['close']) - np.log(df_all['ma_10'])
df_all['ma_20'] = df_all.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['close'].mean().values # ma until x_t-1
df_all['ma_ratio_20'] = np.log(df_all['close']) - np.log(df_all['ma_20'])
# z-return 1d
df_all['ret_1_ma_20'] = df_all.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_1'].mean().values # ma until x_t-1
df_all['ret_1_std_20'] = df_all.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_1'].std().values # ma until x_t-1
df_all['z_score_ret_1'] = np.clip((df_all['ret_1'] - df_all['ret_1_ma_20']) / (df_all['ret_1_std_20'] + EPS), -5, 5)
# z-return 5d
df_all['ret_5_ma_20'] = df_all.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_5'].mean().values # ma until x_t-1
df_all['ret_5_std_20'] = df_all.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_5'].std().values # ma until x_t-1
df_all['z_score_ret_5'] = np.clip((df_all['ret_5'] - df_all['ret_5_ma_20']) / (df_all['ret_5_std_20'] + EPS), -5, 5)
# z-volume of (log(price * Volume))
df_all['vol_log'] = np.log1p(df_all['volume'] * df_all['close']) # cash volume
df_all['vol_log_ma_20'] = df_all.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['vol_log'].mean().values # ma until x_t-1
df_all['vol_log_std_20'] = df_all.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['vol_log'].std().values # ma until x_t-1
df_all['z_vol'] = np.clip((df_all['vol_log'] - df_all['vol_log_ma_20']) / (df_all['vol_log_std_20'] + EPS), -5, 5)
# z-volatility
df_all['std_20_mean_20'] = df_all.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_1_std_20'].mean().values # ma until x_t-1
df_all['std_20_std_20'] = df_all.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_1_std_20'].std().values # ma until x_t-1
df_all['z-volatility'] = np.clip((df_all['ret_1_std_20'] - df_all['std_20_mean_20']) / (df_all['std_20_std_20'] + EPS), -5, 5)
# dividends normalized ma 20
df_all['div_norm'] = np.log1p(df_all['dividends'] / df_all['ma_20'])

# 4. exclude strange stocks
BASE_COLS = ['date', 'isin']
X_FEATURES = ['ret_1', 'ret_5', 'ret_20', 'RSI', 'ma_ratio_5', 'ma_ratio_10', 'ma_ratio_20', 
              'z_score_ret_1', 'z_score_ret_5', 'z_vol', 'z-volatility', 'div_norm']
X_FEATURES_INDICES = ['dax_ret_1', 'dax_ret_5', 'dax_ma_ratio_20', 'dax_z-volatility', 
                      'msci_ret_1', 'msci_ret_5', 'msci_ma_ratio_20', 'msci_z-volatility']
Y_FEATURES = ['class']
df_all[BASE_COLS + X_FEATURES + Y_FEATURES + ['close', 'volume']].to_csv(PATH_DATA + FILE_CLASS, index=False)

THRES_MIN_DAYS = 200
# load data
df_all = pd.read_csv(PATH_DATA + FILE_CLASS)
df_all['date'] = pd.to_datetime(df_all['date'])
df_all['year'] = df_all['date'].dt.year
df_all['month'] = df_all['date'].dt.month
# 4.1. calculate no of trading days
df_all['n_days'] = df_all.groupby('isin')['close'].transform('count')
df_all['few_days'] = np.where(df_all['n_days'] < THRES_MIN_DAYS, 1, 0)
# 4.6 stocks with less than 2 orders in a avg. month movement in current year 
REL_YEAR = [(dt.datetime.today().year)]
MIN_SALES_MONTH = 2
months = dt.datetime.today().month
if dt.datetime.today().month < 3: # if just 2 month up to now, take also full last year
    REL_YEAR.append(dt.datetime.today().year - 1)
    months += 12
df_vol = df_all.loc[df_all['year'].isin(REL_YEAR)].groupby(['isin', 'year']).agg(vol_mean=('volume', 'mean'), vol_sum=('volume', 'sum')).reset_index()
df_vol = df_vol.loc[df_vol['vol_sum'] < months * MIN_SALES_MONTH]
df_vol['low_volume'] = 1 
df_all = df_all.merge(df_vol[['isin', 'low_volume']], on='isin', how='left')
df_all['low_volume'] = df_all['low_volume'].fillna(0)
# 4.7 stocks with big data gaps
THRES_GAP = 8
df_all['date_shift'] = df_all.groupby('isin')['date'].transform('shift', 1)
df_all['date_delta'] = df_all['date'] - df_all['date_shift']
df_all['date_delta_max'] = df_all.groupby('isin')['date_delta'].transform('max')
df_all['date_gap'] = np.where(df_all['date_delta_max'] >= pd.Timedelta(THRES_GAP, 'days'), 1, 0)

# 4.8. exclude stocks
df_all['exclude_lstm'] = np.where(df_all['few_days'] + df_all['low_volume']  + df_all['date_gap'] < 1, 0, 1)
df_exclude = df_all.loc[df_all['exclude_lstm'] == 1][['isin', 'exclude_lstm']].drop_duplicates()
df_exclude.to_csv(PATH_DATA_BASE + FILE_EXCLUDE, index=False)
# sort by columns for latter grouping indices
print("before cleaning:", df_all.shape[0])
df_all = df_all.loc[df_all['exclude_lstm'] == 0][BASE_COLS + X_FEATURES + Y_FEATURES].sort_values(['isin', 'date']).dropna().reset_index(drop=True)
print("after cleaning:", df_all.shape[0])

# 5. add the indices columns
# 5.1 load data
df_ind = pd.read_csv(PATH_DATA + FILE_INDICES).clean_names(strip_underscores=True)
df_ind['date'] = df_ind['date'].astype(str).str[:11]
df_ind['date'] = pd.to_datetime(df_ind['date'])
# 5.2 feature engingeering
# log returns
df_ind['ret_1'] = df_ind.groupby('isin')['close'].transform(lambda x: np.log(x) - np.log(x.shift(1)))
df_ind['ret_5'] = df_ind.groupby('isin')['close'].transform(lambda x: np.log(x) - np.log(x.shift(5)))
# rolling vola z score
df_ind['ret_1_std_20'] = df_ind.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_1'].std().values # ma until x_t-1
df_ind['std_20_mean_20'] = df_ind.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_1_std_20'].mean().values # ma until x_t-1
df_ind['std_20_std_20'] = df_ind.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_1_std_20'].std().values # ma until x_t-1
df_ind['z-volatility'] = np.clip((df_ind['ret_1_std_20'] - df_ind['std_20_mean_20']) / (df_ind['std_20_std_20'] + EPS), -5, 5)
# ma-ratios
df_ind['ma_20'] = df_ind.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['close'].mean().values # ma until x_t-1
df_ind['ma_ratio_20'] = np.log(df_ind['close']) - np.log(df_ind['ma_20'])
# 5.3 prepare dataframe for join
df_ind_wide = df_ind.pivot(index='date', columns='isin', values=['ret_1', 'ret_5', 'z-volatility', 'ma_ratio_20']).reset_index()
df_ind_wide.columns = [col[1] + "_" + col[0] if col[0] != 'date' else col[0] for col in df_ind_wide.columns]
df_ind_wide.ffill(inplace=True)
# 5.4 add indices to df_all and ffill nas
print(df_all.shape[0])
df_all = df_all.merge(df_ind_wide, on='date', how='left')
df_all[X_FEATURES_INDICES] = df_all.groupby('isin')[X_FEATURES_INDICES].transform('ffill')
print(df_all.shape[0])
# 5.5 calculate cross returns
df_all['dax_ret_1_cross'] = df_all['ret_1'] - df_all['dax_ret_1']
df_all['dax_ret_5_cross'] = df_all['ret_5'] - df_all['dax_ret_5']
df_all['msci_ret_1_cross'] = df_all['ret_1'] - df_all['msci_ret_1']
df_all['msci_ret_5_cross'] = df_all['ret_5'] - df_all['msci_ret_5']
X_FEATURES_INDICES += ['dax_ret_1_cross', 'dax_ret_5_cross', 'msci_ret_1_cross', 'msci_ret_5_cross']

# 6. save final dataframe
# 6.1 cast dtypes to save RAM
df_all = df_all.astype({'date':'datetime64[ns]', 'isin':'str'})
for col in X_FEATURES + X_FEATURES_INDICES:
        df_all[col] = df_all[col].astype('float32')
df_all['class'] = df_all['class'].astype(int)

# 6.2 order dataframe an save
print(df_all.shape[0])
df_all = df_all[BASE_COLS + X_FEATURES + X_FEATURES_INDICES + Y_FEATURES].sort_values(['isin', 'date']).dropna().reset_index(drop=True)
print(df_all.shape[0])
# 6.3 save dataframe for sequentiation (in colab)
df_all.to_csv(PATH_DATA + FILE_FINAL_IND, index=False)

