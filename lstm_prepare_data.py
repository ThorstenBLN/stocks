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

plt.rcParams['figure.dpi'] = 600

PATH_DATA = "./data_lstm/"
FILE_HIST = "prices_historic.csv"
FILE_CLASS =  "prices_class.csv"
FILE_FINAL = "data_lstm.csv"
FILE_FINAL_IND = "data_lstm_ind_4_classes.csv"
FILE_INDICES = "prices_historic_indices.csv"
FIRST_YEAR = 2020

# 1. load all data
df_all = pd.read_csv(PATH_DATA + FILE_HIST).clean_names(strip_underscores=True)
df_all['date'] = df_all['date'].astype(str).str[:11]
df_all['date'] = pd.to_datetime(df_all['date'])
df_all = df_all.loc[df_all['date'].dt.year >= FIRST_YEAR][['data_date', 'symbol', 'isin', 
                                                           'date', 'close', 'volume', 'open', 
                                                           'high', 'low', 'dividends', 'stock_splits', 
                                                           'adj_close', 'capital_gains']].sort_values(['isin', 'date'])

# 2. calculate the class for each timeframe (following 4 weeks development of price)
# 2.1 calculate the individual classes
THRES_MAX = [0.05, 0, -0.05]
THRES_MEAN = [0.025, 0, -0.025]
THRES_MOM = [0.02, 0, -0.02]
# calculate rolling means and developments
df_all.sort_values(['isin', 'date'], ascending=[True, False], inplace=True)
df_all['max'] = df_all.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['close'].max().values
df_all['mean_20'] = df_all.groupby('isin').rolling(20, min_periods=20, center=False, closed='left')['close'].mean().values
df_all['mean_10'] = df_all.groupby('isin').rolling(10, min_periods=10, center=False, closed='left')['close'].mean().values
df_all['max_pc'] = df_all['max'] / df_all['close'] - 1
df_all['mean_pc'] = df_all['mean_20'] / df_all['close'] - 1
df_all['mom_pc'] = 2 * df_all['mean_20'] / df_all['mean_10'] - 2 # calculate development news part vs. older part in window

# 2.2 final score and class heuristic
df_all['max_score'] = np.where(df_all['max_pc'].isna(), np.nan,
                                np.where(df_all['max_pc'] > THRES_MAX[0], 3, 
                                np.where(df_all['max_pc'] > THRES_MAX[1], 2,  
                                np.where(df_all['max_pc'] > THRES_MAX[2], 1, 0))))
df_all['mean_score'] = np.where(df_all['mean_pc'].isna(), np.nan,
                                np.where(df_all['mean_pc'] > THRES_MEAN[0], 3, 
                                np.where(df_all['mean_pc'] > THRES_MEAN[1], 2, 
                                np.where(df_all['mean_pc'] > THRES_MEAN[2], 1, 0))))
df_all['mom_score'] = np.where(df_all['mom_pc'].isna(), np.nan, 
                                np.where(df_all['mom_pc'] > THRES_MOM[0], 3, 
                                np.where(df_all['mom_pc'] > THRES_MOM[1], 2, 
                                np.where(df_all['mom_pc'] > THRES_MOM[2], 1, 0))))

df_all['score'] = (df_all['max_score'] + df_all['mean_score'] + df_all['mom_score'])
df_all['class'] = np.round(df_all['score'] / 3, 0)
df_all.sort_values(['isin', 'date'], inplace=True)

# 3. feature engineering
df_all['pre_mean_30'] = df_all.groupby('isin').rolling(30, min_periods=30, center=False)['close'].mean().values
df_all['pre_mean_15'] = df_all.groupby('isin').rolling(15, min_periods=15, center=False)['close'].mean().values
df_all['pre_mean_10'] = df_all.groupby('isin').rolling(10, min_periods=10, center=False)['close'].mean().values
df_all['pre_mean_5'] = df_all.groupby('isin').rolling(5, min_periods=5, center=False)['close'].mean().values
df_all['pre_mean_2'] = df_all.groupby('isin').rolling(2, min_periods=2, center=False)['close'].mean().values
df_all.to_csv(PATH_DATA + FILE_CLASS, index=False)

# # 4. get train and test data
# BASE_COLS = ['date', 'isin']
# X_FEATURES = ['close', 'pre_mean_30', 'pre_mean_15', 'pre_mean_10', 'pre_mean_5', 'pre_mean_2']
# Y_FEATURES = ['class']
# df_all = pd.read_csv(PATH_DATA + FILE_CLASS)
# df_all = df_all[BASE_COLS + X_FEATURES + Y_FEATURES].dropna()
# df_all['date'] = pd.to_datetime(df_all['date'])
# df_all = df_all.astype({'date':'datetime64[ns]', 'isin':'str', 'close':'float32', 'pre_mean_30':'float32', 'pre_mean_15':'float32', 'pre_mean_10':'float32', 'pre_mean_5':'float32', 'pre_mean_2':'float32', 'class':'int8'})
# X_train, y_train, X_test, y_test = f.create_sequences(df_all, 'close', X_FEATURES, 
#                                                       Y_FEATURES, 40, 80, groupby_column='isin')

# 4. exclude strange stocks
BASE_COLS = ['date', 'isin']
X_FEATURES = ['close', 'open', 'high', 'low', 'dividends', 'pre_mean_30', 'pre_mean_15', 'pre_mean_10', 'pre_mean_5', 'pre_mean_2', 'volume']
X_FEATURES_INDICES = ['dax_close', 'dax_mean_5', 'dax_mean_10', 'msci_close', 'msci_mean_5', 'msci_mean_10']
Y_FEATURES = ['class']
THRES_MINI_STOCKS = 0.1 # mean price in qrt
THRES_MOVEMENT = 0.0005 # STD / MEAN ration in qrt -> price doesn't move in avg by 0.05% per day
THRES_MIN_DAYS = 200
# load data
df_all = pd.read_csv(PATH_DATA + FILE_CLASS)

# 4.1 add features for excluding stocks
df_all['date'] = pd.to_datetime(df_all['date'])
df_all['year'] = df_all['date'].dt.year
df_all['month'] = df_all['date'].dt.month
seasons = {month:((month-1) // 3) + 1 for month in df_all['month'].unique()}
df_all['season'] = df_all['month'].map(seasons)
df_all['mean_season'] = df_all.groupby(['isin', 'year', 'season'])['close'].transform('mean')
df_all['std_season'] = df_all.groupby(['isin', 'year', 'season'])['close'].transform('std')
df_all['mean_std_ratio'] = df_all['std_season'] / df_all['mean_season']
# 4.2. mark all penny stocks
df_all['mini_stock'] = np.where((df_all['mean_season'].isna()) | (df_all['mean_season'] < THRES_MINI_STOCKS), 1, 0)
df_all['mini_stock'] = df_all.groupby('isin')['mini_stock'].transform('max')
# 4.3. mark all nearly no price changes
df_all['no_moves'] = np.where((df_all['mean_std_ratio'].isna()) | (np.abs(df_all['mean_std_ratio']) < THRES_MOVEMENT), 1, 0)
df_all['no_moves'] = df_all.groupby('isin')['no_moves'].transform('max')
# 4.4. calculate no of trading days
df_all['n_days'] = df_all.groupby('isin')['close'].transform('count')
df_all['few_days'] = np.where(df_all['n_days'] < THRES_MIN_DAYS, 1, 0)
# 4.6 stocks with less than 2 orders in a month movement in current year
df_all['year'] = df_all['date'].dt.year
REL_YEAR = [(dt.datetime.today().year)]
MIN_SALES_MONTH = 2
months = dt.datetime.today().month
if dt.datetime.today().month < 3:
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
df_all['exclude'] = np.where(df_all['mini_stock'] + df_all['no_moves'] + df_all['few_days'] + df_all['low_volume']  + df_all['date_gap'] < 1, 0, 1)
df_exclude = df_all.loc[df_all['exclude'] == 1][['isin', 'exclude']].drop_duplicates()
df_exclude.to_csv("./data/exclude_isin.csv", index=False)
# sort by columns for latter grouping indices
df_all = df_all.loc[df_all['exclude'] == 0][BASE_COLS + X_FEATURES + Y_FEATURES].sort_values(['isin', 'date']).dropna().reset_index(drop=True)

# 5. add the indices columns
# 5.1 load data
df_ind = pd.read_csv(PATH_DATA + FILE_INDICES).clean_names(strip_underscores=True)
df_ind['date'] = df_ind['date'].astype(str).str[:11]
df_ind['date'] = pd.to_datetime(df_ind['date'])
# 5.2 add means
df_ind['mean_10'] = df_ind.groupby('isin').rolling(10, min_periods=10, center=False)['close'].mean().values
df_ind['mean_5'] = df_ind.groupby('isin').rolling(5, min_periods=5, center=False)['close'].mean().values
# 5.3 prepare dataframe for join
df_ind_wide = df_ind.pivot(index='date', columns='isin', values=['close', 'mean_10', 'mean_5']).reset_index()
df_ind_wide.columns = [col[1] + "_" + col[0] if col[0] != 'date' else col[0] for col in df_ind_wide.columns]
df_ind_wide.ffill(inplace=True)
# 5.4 add indices to df_all and ffill nas
print(df_all.shape[0])
df_all = df_all.merge(df_ind_wide, on='date', how='left')
df_all[X_FEATURES_INDICES] = df_all.groupby('isin')[X_FEATURES_INDICES].transform('ffill')
print(df_all.shape[0])
# 6. save final dataframe
# 6.1 cast dtypes to save RAM
df_all = df_all.astype({'date':'datetime64[ns]', 'isin':'str', 'close':'float32', 
                        'open':'float32', 'high':'float32', 'low':'float32', 
                        'dividends':'float32', 'pre_mean_30':'float32', 
                        'pre_mean_15':'float32', 'pre_mean_10':'float32', 'pre_mean_5':'float32', 
                        'pre_mean_2':'float32', 'volume':'float32', 'class':'int8', 'dax_close':'float32', 'msci_close':'float32', 
                        'dax_mean_10':'float32', 'msci_mean_10':'float32', 'dax_mean_5':'float32', 
                        'msci_mean_5':'float32'})
# 6.2 order dataframe an save
df_all = df_all[BASE_COLS + X_FEATURES + X_FEATURES_INDICES + Y_FEATURES].sort_values(['isin', 'date']).dropna().reset_index(drop=True)
print(df_all.shape[0])
# 6.3 save dataframe for sequentiation (in colab)
df_all.to_csv(PATH_DATA + FILE_FINAL_IND, index=False)

