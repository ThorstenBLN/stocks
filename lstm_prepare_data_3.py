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
FILE_HIST = "prices_historic_10.csv" # "prices_historic.csv"
FILE_CLASS =  "prices_class.csv"
FILE_FINAL = "data_lstm.csv"
FILE_FINAL_IND = "data_lstm_ind_2_classes_q175.csv"
FILE_INDICES = "prices_historic_indices_10.csv" # "prices_historic_indices.csv"
FILE_EXCLUDE = "exclude_isin.csv"
# Filter relevant stocks
FIRST_YEAR = 2016
N_YEARS_VOLUME = 11
NLARGEST_STOCKS = 2000
# classification
DAYS_FUTURE = 3
THRES_QTL = 0.25
# save division epsilon
EPS = 10e-8

def calculate_beta(df, group_col, col1, ind_col, win_len=20): # checked in excel works. includes current price
    '''calculates the covariance of col1 and ind_col and var of ind_col over a rolling window. 
    returns beta with ind_col as index'''
    beta = []
    for i, win in enumerate(df.groupby(group_col)[[col1, ind_col]].rolling(win_len, center=False, closed='left')):
        rol_col1 = win[col1] - win[col1].mean(skipna=False)
        rol_ind_col = win[ind_col] - win[ind_col].mean(skipna=False)
        beta.append(np.sum(rol_col1 * rol_ind_col) / np.sum(rol_ind_col**2))
        if i % 10000 == 0:
             print(i)
    return np.array(beta)


# 1. load all data
df_all = pd.read_csv(PATH_DATA + FILE_HIST).clean_names(strip_underscores=True)
df_all['date'] = df_all['date'].astype(str).str[:11]
df_all['date'] = pd.to_datetime(df_all['date'])
df_all = df_all.loc[df_all['date'].dt.year >= FIRST_YEAR][['data_date', 'symbol', 'isin', 
                                                           'date', 'close', 'volume', 'open', 
                                                           'high', 'low', 'dividends', 'stock_splits', 
                                                           'adj_close', 'capital_gains']].sort_values(['isin', 'date'])

# 2. filter on relevant stocks
# 2.1 take the 2000 with highest Volume this and last year
df_vol = df_all.loc[df_all['date'].dt.year >= dt.datetime.now().year - N_YEARS_VOLUME + 1].groupby('isin')['volume'].mean().nlargest(NLARGEST_STOCKS).reset_index()
# 2.2 delete pennystorcks
df_price = df_all.groupby('isin')['close'].mean()
df_price = df_price[df_price > 5].reset_index()
# 2.3 stocks with least gaps
df_gap = df_all.groupby('isin')['close'].count()
df_gap = df_gap[df_gap > df_gap.max() * 0.95].reset_index()
# 2.4 delete stocks with biggest gaps
df_all['days_diff'] = df_all.groupby('isin')['date'].diff()
df_biggap = df_all.groupby('isin')['days_diff'].max().reset_index()
df_biggap = df_biggap.loc[df_biggap['days_diff'] < df_biggap['days_diff'].quantile(0.95)]
# 2.5 delete stocks with many no trade days (definded as bad isin)
df_volgap = df_all.loc[df_all['volume'] == 0].groupby('isin')['volume'].count().reset_index()
df_volgap = df_volgap.loc[df_volgap['volume'] > df_volgap['volume'].quantile(0.75)]
# 2.6 delete stocks with too long periods without trade (definded as bad isin)
df_longgap = df_all.loc[df_all['volume'] == 0][['isin', 'volume','date']].copy()
df_longgap['prev_gap'] = df_longgap.groupby('isin')['date'].diff()
df_longgap['start_gap'] = np.where(df_longgap['prev_gap'].dt.days > 3, 1, 0)
df_longgap['gap_group'] = df_longgap.groupby('isin')['start_gap'].cumsum()
df_longgap = df_longgap.groupby(['isin', 'gap_group'])['date'].count().reset_index()
df_longgap = df_longgap.groupby('isin')['date'].max().reset_index()
df_longgap = df_longgap.loc[df_longgap['date'] > df_longgap['date'].quantile(0.75)]
# 2.7 get good volume isins
good_volgap_set = set(df_all['isin'].unique()) - set(df_volgap['isin'].unique()) - set(df_longgap['isin'].unique())

# 2.4 combine filters
good_isins = set(df_vol['isin']) & set(df_price['isin']) & set(df_gap['isin']) & set(df_biggap['isin']) & good_volgap_set
df_sel = df_all.loc[df_all['isin'].isin(good_isins)].copy()

# # 3. calculate the class for each timeframe (following 1 week avg. of price)
df_sel['close_future'] = df_sel.groupby('isin')['close'].transform(lambda x: x.shift(-DAYS_FUTURE))
df_sel['close_mean'] = df_sel.groupby('isin').rolling(DAYS_FUTURE, min_periods=DAYS_FUTURE, center=False, closed='right')['close_future'].max().values
df_sel['target'] = np.log(df_sel['close_mean']) - np.log(df_sel['close']) 
print(df_sel['target'].describe())
# thres = (np.abs(df_sel['target'].quantile(THRES_QTL)) + df_sel['target'].quantile(1-THRES_QTL)) / 2
# print(thres)
thres_low = df_sel['target'].quantile(THRES_QTL)
thres_up = df_sel['target'].quantile(1-THRES_QTL)
print(thres_up, thres_low)
# df_sel['class'] = np.where(df_sel['target'] > thres, 0, np.where(df_sel['target'] < -thres, 1, 2))
df_sel['class'] = np.where(df_sel['target'] > thres_up, 2, np.where(df_sel['target'] < thres_low, 0, 1))
print(df_sel['class'].value_counts())
# --> class 0: clear up, class 1: clear down, class 2: flat (discard)
df_sel.sort_values(['isin', 'date'], inplace=True)
df_sel.to_csv(PATH_DATA + FILE_CLASS, index=False)

# 4. feature engineering
# calculate returns 
df_sel['ret_1'] = df_sel.groupby('isin')['close'].transform(lambda x: np.log(x) - np.log(x.shift(1)))
df_sel['ret_5'] = df_sel.groupby('isin')['close'].transform(lambda x: np.log(x) - np.log(x.shift(5)))
df_sel['ret_20'] = df_sel.groupby('isin')['close'].transform(lambda x: np.log(x) - np.log(x.shift(20)))
df_sel['ret_60'] = df_sel.groupby('isin')['close'].transform(lambda x: np.log(x) - np.log(x.shift(60)))
# vola
df_sel['vola_20'] = df_sel.groupby('isin')['ret_1'].rolling(20, min_periods=20, center=False, closed='left').std().values # cash volume
df_sel['vola_ratio'] = df_sel['vola_20'] / df_sel.groupby('isin')['vola_20'].rolling(60, min_periods=60, center=False, closed='left').mean().values# cash volume
# Volume
df_sel['volume_ratio'] = df_sel['volume'] / df_sel.groupby('isin')['volume'].rolling(20, min_periods=20, center=False, closed='left').mean().values# cash volume
df_sel['volume_5'] = df_sel.groupby('isin')['volume'].rolling(5, min_periods=5, center=False, closed='left').mean().values
df_sel['volume_5_log'] = np.log1p(df_sel['volume_5'])
df_sel['volume_5_mean'] = df_sel.groupby('isin')['volume_5_log'].transform().mean()
df_sel['volume_5_std'] = df_sel.groupby('isin')['volume_5_log'].transform().std()
df_sel['volume_5_norm'] = (df_sel['volume_5_log'] - df_sel['volume_5_mean']) / (df_sel['volume_5_std'] + EPS)
df_sel['volume_20'] = df_sel.groupby('isin')['volume'].rolling(20, min_periods=20, center=False, closed='left').mean().values
df_sel['volume_trend'] = df_sel['volume_5'] / df_sel['volume_20']

# price patterns
df_sel['hi_lo_ratio'] = (df_sel['high'] - df_sel['low']) / df_sel['close']
df_sel['close_pos'] = (df_sel['close'] - df_sel['low']) / (df_sel['high'] - df_sel['low'] + EPS)
# market regime
df_sel['market_ret_1'] = df_sel.groupby('date')['ret_1'].transform('mean')
df_sel['market_vola'] = df_sel.groupby('date')['ret_1'].transform('std')
df_sel['beta'] = np.clip(calculate_beta(df_sel, 'isin', 'ret_1', 'market_ret_1', 20), -5, 10)
# RELATIVE STRENGTH
df_sel['rank_ret_20'] = df_sel.groupby('date')['ret_20'].rank(pct=True)  # percentile rank
# RSI
N = 14
df_sel['u'] = df_sel.groupby('isin')['close'].transform(lambda x: x - x.shift(1))
df_sel['u'] = np.where(df_sel['u'] < 0, 0, df_sel['u'])
df_sel['d'] = df_sel.groupby('isin')['close'].transform(lambda x: x.shift(1) - x)
df_sel['d'] = np.where(df_sel['d'] < 0, 0, df_sel['d'])
df_sel['ewm_u'] = df_sel.groupby(['isin'])['u'].ewm(alpha=1/N, ignore_na=True, min_periods=20).mean().values
df_sel['ewm_d'] = df_sel.groupby(['isin'])['d'].ewm(alpha=1/N, ignore_na=True, min_periods=20).mean().values
df_sel['RSI'] = 1 - 1 / (1 + df_sel['ewm_u'] / df_sel['ewm_d'])
# calculate moving average ratios
df_sel['ma_5'] = df_sel.groupby('isin').rolling(5, min_periods=5, center=False, closed='left')['close'].mean().values # ma until x_t-1
df_sel['ma_ratio_5'] = np.log(df_sel['close']) - np.log(df_sel['ma_5'])
df_sel['ma_10'] = df_sel.groupby('isin').rolling(10, min_periods=10, center=False, closed='left')['close'].mean().values # ma until x_t-1
df_sel['ma_ratio_10'] = np.log(df_sel['close']) - np.log(df_sel['ma_10'])
df_sel['ma_20'] = df_sel.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['close'].mean().values # ma until x_t-1
df_sel['ma_ratio_20'] = np.log(df_sel['close']) - np.log(df_sel['ma_20'])
# z-return 1d
df_sel['ret_1_ma_20'] = df_sel.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_1'].mean().values # ma until x_t-1
df_sel['ret_1_std_20'] = df_sel.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_1'].std().values # ma until x_t-1
df_sel['z_score_ret_1'] = np.clip((df_sel['ret_1'] - df_sel['ret_1_ma_20']) / (df_sel['ret_1_std_20'] + EPS), -5, 5)
# z-return 5d
df_sel['ret_5_ma_20'] = df_sel.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_5'].mean().values # ma until x_t-1
df_sel['ret_5_std_20'] = df_sel.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_5'].std().values # ma until x_t-1
df_sel['z_score_ret_5'] = np.clip((df_sel['ret_5'] - df_sel['ret_5_ma_20']) / (df_sel['ret_5_std_20'] + EPS), -5, 5)
# z-volume of (log(price * Volume))
df_sel['vol_log'] = np.log1p(df_sel['volume'] * df_sel['close']) # cash volume
df_sel['vol_log_ma_20'] = df_sel.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['vol_log'].mean().values # ma until x_t-1
df_sel['vol_log_std_20'] = df_sel.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['vol_log'].std().values # ma until x_t-1
df_sel['z_vol'] = np.clip((df_sel['vol_log'] - df_sel['vol_log_ma_20']) / (df_sel['vol_log_std_20'] + EPS), -5, 5)
# z-volatility
df_sel['std_20_mean_20'] = df_sel.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_1_std_20'].mean().values # ma until x_t-1
df_sel['std_20_std_20'] = df_sel.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['ret_1_std_20'].std().values # ma until x_t-1
df_sel['z-volatility'] = np.clip((df_sel['ret_1_std_20'] - df_sel['std_20_mean_20']) / (df_sel['std_20_std_20'] + EPS), -5, 5)
# dividends normalized ma 20
df_sel['div_norm'] = np.log1p(df_sel['dividends'] / df_sel['ma_20'])

# 4. exclude strange stocks
BASE_COLS = ['date', 'isin']
X_FEATURES = ['ret_1', 'ret_5', 'ret_20', 'ret_60', 'vola_20', 'vola_ratio', 
              'volume_ratio', 'volume_5', 'volume_20', 'volume_trend', 'hi_lo_ratio',
                'close_pos', 'market_ret_1', 'market_vola', 'beta', 'rank_ret_20',
              'RSI', 'ma_ratio_5', 'ma_ratio_10', 'ma_ratio_20', 
              'z_score_ret_1', 'z_score_ret_5', 'z_vol', 'z-volatility', 'div_norm']
X_FEATURES_INDICES = ['dax_ret_1', 'dax_ret_5', 'dax_ma_ratio_20', 'dax_z-volatility', 
                      'msci_ret_1', 'msci_ret_5', 'msci_ma_ratio_20', 'msci_z-volatility']
Y_FEATURES = ['class']
df_sel[BASE_COLS + X_FEATURES + Y_FEATURES + ['close', 'volume']].to_csv(PATH_DATA + FILE_CLASS, index=False)

# THRES_MIN_DAYS = 200
# # load data
# df_sel = pd.read_csv(PATH_DATA + FILE_CLASS)
# df_sel['date'] = pd.to_datetime(df_all['date'])
# df_all['year'] = df_all['date'].dt.year
# df_all['month'] = df_all['date'].dt.month
# # 4.1. calculate no of trading days
# df_all['n_days'] = df_all.groupby('isin')['close'].transform('count')
# df_all['few_days'] = np.where(df_all['n_days'] < THRES_MIN_DAYS, 1, 0)
# # 4.6 stocks with less than 2 orders in a avg. month movement in current year 
# REL_YEAR = [(dt.datetime.today().year)]
# MIN_SALES_MONTH = 2
# months = dt.datetime.today().month
# if dt.datetime.today().month < 3: # if just 2 month up to now, take also full last year
#     REL_YEAR.append(dt.datetime.today().year - 1)
#     months += 12
# df_vol = df_all.loc[df_all['year'].isin(REL_YEAR)].groupby(['isin', 'year']).agg(vol_mean=('volume', 'mean'), vol_sum=('volume', 'sum')).reset_index()
# df_vol = df_vol.loc[df_vol['vol_sum'] < months * MIN_SALES_MONTH]
# df_vol['low_volume'] = 1 
# df_all = df_all.merge(df_vol[['isin', 'low_volume']], on='isin', how='left')
# df_all['low_volume'] = df_all['low_volume'].fillna(0)
# # 4.7 stocks with big data gaps
# THRES_GAP = 8
# df_all['date_shift'] = df_all.groupby('isin')['date'].transform('shift', 1)
# df_all['date_delta'] = df_all['date'] - df_all['date_shift']
# df_all['date_delta_max'] = df_all.groupby('isin')['date_delta'].transform('max')
# df_all['date_gap'] = np.where(df_all['date_delta_max'] >= pd.Timedelta(THRES_GAP, 'days'), 1, 0)

# # 4.8. exclude stocks
# df_all['exclude_lstm'] = np.where(df_all['few_days'] + df_all['low_volume']  + df_all['date_gap'] < 1, 0, 1)
# df_exclude = df_all.loc[df_all['exclude_lstm'] == 1][['isin', 'exclude_lstm']].drop_duplicates()
# df_exclude.to_csv(PATH_DATA_BASE + FILE_EXCLUDE, index=False)
# # sort by columns for latter grouping indices
# print("before cleaning:", df_all.shape[0])
# df_all = df_all.loc[df_all['exclude_lstm'] == 0][BASE_COLS + X_FEATURES + Y_FEATURES].sort_values(['isin', 'date']).dropna().reset_index(drop=True)
# print("after cleaning:", df_all.shape[0])

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
# 5.4 add indices to df_sel and ffill nas
print(df_sel.shape[0])
df_sel = df_sel.merge(df_ind_wide, on='date', how='left')
print("nas: ", df_sel.isna().sum().sum())
df_sel[X_FEATURES_INDICES] = df_sel.groupby('isin')[X_FEATURES_INDICES].transform('ffill')
print(df_sel.shape[0])
# 5.5 calculate cross returns
df_sel['dax_ret_1_cross'] = df_sel['ret_1'] - df_sel['dax_ret_1']
df_sel['dax_ret_5_cross'] = df_sel['ret_5'] - df_sel['dax_ret_5']
df_sel['msci_ret_1_cross'] = df_sel['ret_1'] - df_sel['msci_ret_1']
df_sel['msci_ret_5_cross'] = df_sel['ret_5'] - df_sel['msci_ret_5']
X_FEATURES_INDICES += ['dax_ret_1_cross', 'dax_ret_5_cross', 'msci_ret_1_cross', 'msci_ret_5_cross']
# 6. save final dataframe
# 6.1 cast dtypes to save RAM
df_sel = df_sel.astype({'date':'datetime64[ns]', 'isin':'str'})
for col in X_FEATURES + X_FEATURES_INDICES:
        df_sel[col] = df_sel[col].astype('float32')
df_sel['class'] = df_sel['class'].astype(int)

# 6.2 order dataframe an save
print(df_sel.shape[0])
df_sel = df_sel[BASE_COLS + ["close"] + X_FEATURES + X_FEATURES_INDICES + Y_FEATURES].sort_values(['isin', 'date']).dropna().reset_index(drop=True)
print(df_sel.shape[0])
# 6.3 save dataframe for sequentiation (in colab)
df_sel.to_csv(PATH_DATA + FILE_FINAL_IND, index=False)