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

def main():
    warnings.simplefilter('ignore', 'FutureWarning')
    logging.basicConfig(filename='./data/data_pipeling.log', level=logging.INFO)

    PATH = "./data/"
    FILE_SYMBOLS = "symbols.xlsx"
    FILE_DATES = "dates.xlsx"
    FILE_KGV = "kgv_5y.xlsx"
    FILE_DATA = "data_all.xlsx"
    FILE_DATA_1 = "data_all_1.xlsx"
    FILE_RESULT_DAY  = "result.xlsx"

    INDEX_SYMBOL = "^990100-USD-STRD" #"^GDAXI"
    NA_PENALTY = -0.333
    DAYS_THRES = 85

    # 1. load base data ####################################################################
    time_1 = time.time()
    df_base_orig = pd.read_excel(PATH + FILE_SYMBOLS)
    mask = (df_base_orig['data_all'] == 1) & (df_base_orig['isin'].notna())
    df_base = df_base_orig.loc[mask].copy().reset_index()
    df_dates = pd.read_excel(PATH + FILE_DATES)

    # 3. load data for levermann formual part 2
    # 3.1 base data index and relevant dates #######################################################
    dat_index = yf.Ticker(INDEX_SYMBOL)
    df_index_hist = dat_index.history(period="2y").reset_index()
    df_index_hist['Date'] = df_index_hist['Date'].dt.date
    if df_index_hist.empty:
        print("error occured while loading dax")
        raise SystemExit("stopped due to dax download error")

    DATE_CUR = df_index_hist['Date'].max() 
    DATES = {'cur': DATE_CUR, 
            '3m': (DATE_CUR - pd.DateOffset(months=3)).date(), 
            '6m': (DATE_CUR - pd.DateOffset(months=6)).date(), 
            '12m': (DATE_CUR - pd.DateOffset(months=12)).date(), 
            '18m': (DATE_CUR - pd.DateOffset(months=18)).date()}
    df_index_prices = f.get_hist_prices(df_index_hist, DATES)

    # 3.2 get levermann data fram yfinance api (ca. 30 min / 1000 symbols)
    # get the dates of the last Geschäftszahlen presentation
    df_dates = pd.read_excel(PATH + FILE_DATES)
    df_dates['time_delta'] = (df_dates['date'] - pd.to_datetime('today')).dt.days
    df_dates['date'] = df_dates['date'].dt.date
    df_dates_qrt = df_dates.loc[(df_dates['type'] == 'Quartalszahlen') & (df_dates['time_delta'] <= 0)].copy() 
    df_dates_qrt_rel = df_dates_qrt.sort_values(['time_delta'], ascending=False).groupby(['isin']).head(1).reset_index()
    df_dates_jv = df_dates.loc[(df_dates['type'] == 'Hauptversammlung') & (df_dates['time_delta'] <= 0)].copy() 
    df_dates_jv_rel = df_dates_jv.sort_values(['time_delta'], ascending=False).groupby(['isin']).head(1).reset_index()
    time_2 = time.time()
    print(f"loading files and prepare dates data: {np.round((time_2 - time_1)/60, 2).item()} minutes")
    # download data
    DATA_PC = 0.45
    end = int(df_base.shape[0] * DATA_PC)
    data = []
    for row in df_base.iloc[end:].itertuples():
        if row.Index % 100 == 0:
            print(row.Index, row.symbol)
        qrt_date = df_dates_qrt_rel.loc[df_dates_qrt_rel['isin'] == row.isin]['date']
        jv_date = df_dates_jv_rel.loc[df_dates_jv_rel['isin'] == row.isin]['date']
        data.append(f.get_levermann_data(row, df_index_hist, df_index_prices, DATES, qrt_date, jv_date))
        time.sleep(np.random.uniform(0.6, 1.0))
    df_data = pd.DataFrame(data)
    print("code data levermann finished successfully")
    df_data['data_date'] = pd.to_datetime(df_data['data_date']).dt.date
    df_data_1 = pd.read_excel(PATH + FILE_DATA_1)
    df_data = pd.concat([df_data_1, df_data])
    df_data.to_excel(PATH + FILE_DATA, index=False)
    time_1 = time.time()
    print(f"extract ldata: {np.round((time_1 - time_2)/60, 2).item()} minutes")
    # 4. calculate levermann score #############################################################
    df_kgv = pd.read_excel(PATH + FILE_KGV)
    # df_data = pd.read_excel(PATH + FILE_DATA)
    df_data['forward_kgv'] = np.where(df_data['forward_kgv'] == "Infinity", np.inf, df_data['forward_kgv']).astype('float')
    df_data_complete = df_data.merge(df_kgv, on='isin', how='left')
    print("data merge finished successfully")
    df_result = f.add_levermann_score(df_data_complete, NA_PENALTY)
    df_result.to_excel(PATH + FILE_RESULT_DAY, index=False)
    time_2 = time.time()
    print(f"merge and save data: {np.round((time_2 - time_1)/60, 2).item()} minutes")

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logging.info(f"{dt.datetime.now().strftime('%d.%m.%Y %H:%M:%S')} Exception extract_data_2 main")
        logging.error(err, stack_info=True, exc_info=True)
        print(err)
        sys.exit(1)

