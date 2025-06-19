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
import pickle

def main():
    warnings.simplefilter('ignore', 'FutureWarning')
    logging.basicConfig(filename='./data/data_pipeling.log', level=logging.INFO)

    PATH = "./data/"
    FILE_RESULT_DAY  = "result.xlsx"
    FILE_RESULT = "result_hist.csv"
    FILE_DEPOT = "depot.xisx"
    FILE_MODEL = "bi_lstm_model.pickle"
    MIN_BUY_SCORE = 8

    MIN_TOP = 0.45
    MIN_GOOD = 0.5
    
    # 1. load base data ####################################################################
    time_1 = time.time()
    if os.path.exists(PATH + FILE_DEPOT):
        df_depot = pd.read_excel(PATH + FILE_DEPOT)[['isin','symbol']]
    else:
        df_depot = pd.DataFrame()
    if not os.path.exists(PATH + FILE_RESULT): # for the first time there is no result file
        df_result_hist = pd.DataFrame()
    else:
        df_result_hist = pd.read_csv(PATH + FILE_RESULT)
    df_result = pd.read_excel(PATH + FILE_RESULT_DAY)
    df_isin = df_result.loc[df_result['lev_score'] >= MIN_BUY_SCORE][['isin','symbol']]
    df_isin = pd.concat([df_isin, df_depot]).drop_duplicates().reset_index(drop=True)
    time_2 = time.time()
    print(f"loading files: {np.round((time_2 - time_1)/60, 2).item()} minutes")

    # 2 try to get 3 month of data for each stock
    data = []
    for row in df_isin.iloc[:].itertuples():
        if row.Index % 100 == 0:
            print(row.Index, row.symbol)
        print(row.Index, row.symbol)
        data.append(f.get_historic_data(row, per='6mo'))
        print("error: after data")
        time.sleep(np.random.uniform(0.8, 1.2))
    df_data = pd.concat(data).clean_names(strip_underscores=True)
    df_data['date'] = df_data['date'].astype(str).str[:11]
    df_data['date'] = pd.to_datetime(df_data['date'])
    df_data.sort_values(['isin', 'date'], inplace=True)
    time_1 = time.time()
    print(f"loading isin data: {np.round((time_1 - time_2)/60, 2).item()} minutes")
    # 3. download indices
    df_indices = pd.DataFrame({'symbol':['^GDAXI', "^990100-USD-STRD"], 'isin':['dax', 'msci']}) # ['DE0008469008', 'GB00BJDQQQ59']
    data_ind = []
    for row in df_indices.iloc[:].itertuples():
        if row.Index % 100 == 0:
            print(row.Index, row.symbol)
        data_ind.append(f.get_historic_data(row, per='6mo'))
        time.sleep(np.random.uniform(0.8, 1.2))
    df_ind = pd.concat(data_ind).clean_names(strip_underscores=True)
    df_ind['date'] = df_ind['date'].astype(str).str[:11]
    df_ind['date'] = pd.to_datetime(df_ind['date'])
    df_ind.sort_values(['isin', 'date'], inplace=True)
    time_2 = time.time()
    print(f"loading indices data: {np.round((time_2 - time_1)/60, 2).item()} minutes")

    # 3. feature engineering
    for mean_days in [30, 15, 10, 5, 2]:
        df_data[f'pre_mean_{mean_days}'] = df_data.groupby('isin').rolling(mean_days, min_periods=mean_days, center=False)['close'].mean().values

    # 4. prepare LSTM data
    BASE_COLS = ['date', 'isin']
    X_FEATURES = ['close', 'pre_mean_30', 'pre_mean_15', 'pre_mean_10', 'pre_mean_5', 'pre_mean_2', 'volume']
    X_FEATURES_INDICES = ['dax_close', 'dax_mean_5', 'dax_mean_10', 'msci_close', 'msci_mean_5', 'msci_mean_10']
    df_data = df_data[BASE_COLS + X_FEATURES].sort_values(['isin', 'date']).dropna().reset_index(drop=True)

    # 5. add the indices columns
    # 5.1 add means
    df_ind['mean_10'] = df_ind.groupby('isin').rolling(10, min_periods=10, center=False)['close'].mean().values
    df_ind['mean_5'] = df_ind.groupby('isin').rolling(5, min_periods=5, center=False)['close'].mean().values
    # 5.3 prepare dataframe for join
    df_ind_wide = df_ind.pivot(index='date', columns='isin', values=['close', 'mean_10', 'mean_5']).reset_index()
    df_ind_wide.columns = [col[1] + "_" + col[0] if col[0] != 'date' else col[0] for col in df_ind_wide.columns]
    df_ind_wide.ffill(inplace=True)
    # 5.4 add indices to df_all and ffill nas
    print(df_data.shape[0])
    df_all = df_data.merge(df_ind_wide, on='date', how='left')
    df_all[X_FEATURES_INDICES] = df_all.groupby('isin')[X_FEATURES_INDICES].transform('ffill')
    print(df_all.shape[0])
    # 6. create numpy arrays
    # 6.1 cast dtypes to save RAM
    df_all = df_all.astype({'date':'datetime64[ns]', 'isin':'str', 'close':'float32', 'pre_mean_30':'float32', 
                            'pre_mean_15':'float32', 'pre_mean_10':'float32', 'pre_mean_5':'float32', 
                            'pre_mean_2':'float32', 'volume':'float32', 'dax_close':'float32', 'msci_close':'float32', 
                            'dax_mean_10':'float32', 'msci_mean_10':'float32', 'dax_mean_5':'float32', 
                            'msci_mean_5':'float32'})
    # 6.2 order dataframe an save
    df_all = df_all[BASE_COLS + X_FEATURES + X_FEATURES_INDICES].sort_values(['isin', 'date']).dropna().reset_index(drop=True)
    print(df_all.shape[0])
    time_1 = time.time()
    print(f"prepare data: {np.round((time_1 - time_2)/60, 2).item()} minutes")
    # 6.3 get sequences for model
    X_FEATURES_LSTM = ['close', 'pre_mean_30', 'pre_mean_15', 'pre_mean_10', 'pre_mean_5', 'pre_mean_2', 'msci_close', 'msci_mean_5', 'dax_close', 'dax_mean_5']
    indices_dict = df_all.iloc[:].groupby('isin').indices
    np_all = df_all[X_FEATURES_LSTM].iloc[:].to_numpy()
    win_len = 40
    X_pred, df_pred = f.get_pred_arrays(indices_dict, np_all, win_len, [0, 6, 8])
    time_2 = time.time()
    print(f"prepare lstm arrays: {np.round((time_2 - time_1)/60, 2).item()} minutes")

    # 7 predict stocks 
    # 7.1 load current model
    with open(PATH + FILE_MODEL, 'rb') as f:
        lstm_model = pickle.load(f)
    # 7.2 get predictions
    y_pred = lstm_model.predict(X_pred)
    for i in range(y_pred.shape[1]):
        df_pred[i] = y_pred[:, i]
    df_pred['class'] = np.argmax(y_pred, axis=1)
    df_pred = df_pred.merge(df_all.reset_index(drop=False, names=['end_df'])[['end_df', 'date']], on='end_df', how='left')
    time_1 = time.time()
    print(f"get predictions: {np.round((time_1 - time_2)/60, 2).item()} minutes")

    # 7.3 choose relevant predictions (last 2 days and 1 week ago)
    dates = df_pred.sort_values('date')['date'].unique()
    DATES = [dates[-1], dates[-2], dates[-6]]
    df_pred = df_pred.loc[df_pred['date'].isin(DATES)]

    # 7.4 caculate metrics
    df_pred['good'] = (df_pred[3] + df_pred[2])
    df_pred = df_pred.groupby(['isin']) \
            .agg(mean_top = (3, 'mean'), mean_good = ('good', 'mean')) \
            .reset_index() 
    df_pred['score'] = 10 * (df_pred['mean_top'] - MIN_TOP) + 5 * (df_pred['mean_good'] - MIN_GOOD)

    # 8. add to result file and save file
    df_result = df_result.merge(df_pred, on='isin', how='left')
    df_result['score'] = df_result['score'].fillna(-2)
    df_result['score_tot'] = df_result['lev_score'] + df_result['score']
    df_result.to_excel(PATH + FILE_RESULT_DAY, index=False)

    df_result_tot = pd.concat([df_result_hist, df_result], axis=0).reset_index(drop=True)
    df_result_tot.to_csv(PATH + FILE_RESULT, index=False)

    time_2 = time.time()
    print(f"get scores and save results: {np.round((time_2 - time_1)/60, 2).item()} minutes")

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logging.info(f"{dt.datetime.now().strftime('%d.%m.%Y %H:%M:%S')} Exception extract_lstm_data main")
        logging.error(err, stack_info=True, exc_info=True)
        print(err)
        sys.exit(1)

