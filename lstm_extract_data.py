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
    PATH_DATA = "./data_lstm/"
    FILE_SYMBOLS = "symbols.xlsx"
    FILE_HIST = "prices_historic.csv"
    FILE_HIST_IND = "prices_historic_indices.csv"

    # 1. load base data ####################################################################
    df_symbols = pd.read_excel(PATH + FILE_SYMBOLS)
    mask = (df_symbols['data_all'] == 1) & (df_symbols['isin'].notna())
    df_symbols = df_symbols.loc[mask].reset_index()

    # 2 try to get 5 years of data for each stock
    data = []
    for row in df_symbols.iloc[:].itertuples():
        if row.Index % 100 == 0:
            print(row.Index, row.symbol)
        data.append(f.get_historic_data(row))
        time.sleep(np.random.uniform(0.8, 1.2))
    df_data = pd.concat(data)
    df_data['data_date'] = time.strftime("%Y/%m/%d")
    # save first part of data to file
    df_data.to_csv(PATH_DATA + FILE_HIST)
    print("data captured sucessfully")

    # 3. download indices
    df_indices = pd.DataFrame({'symbol':['^GDAXI', "^990100-USD-STRD"], 'isin':['dax', 'msci']}) # ['DE0008469008', 'GB00BJDQQQ59']
    data_ind = []
    for row in df_indices.iloc[:].itertuples():
        if row.Index % 100 == 0:
            print(row.Index, row.symbol)
        data_ind.append(f.get_historic_data(row))
        time.sleep(np.random.uniform(0.8, 1.2))
    df_data_ind = pd.concat(data_ind)
    df_data_ind['data_date'] = time.strftime("%Y/%m/%d")
    # save first part of data to file
    df_data_ind.to_csv(PATH_DATA + FILE_HIST_IND)
    print("index data captured sucessfully")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logging.info(f"{dt.datetime.now().strftime('%d.%m.%Y %H:%M:%S')} Exception extract_data historic main")
        logging.error(err, stack_info=True, exc_info=True)
        sys.exit(1)
