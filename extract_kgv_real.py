import yfinance as yf
import pandas as pd
import numpy as np
import time
import os
import openpyxl
import datetime as dt
import warnings
import functions as f
import logging
import sys
import finhandler

def main():
    warnings.simplefilter('ignore', 'FutureWarning')
    logging.basicConfig(filename='./data/data_pipeling.log', level=logging.INFO)

    PATH = "./data/"
    FILE_SYMBOLS = "symbols.xlsx"
    FILE_KGV = "kgv_5y_real.xlsx"

    # 1. load symbols
    df_base = pd.read_excel(PATH + FILE_SYMBOLS)

    # 2.2 scrape old KGV (ca. 20 min for 1000 symbols)
    kgv_real = []
    fin_handler = finhandler.Finhandler()
    # relevant years are the last passed 4 years 
    prev_year = dt.datetime.now().year - 1
    REL_YEARS_REAL = [str(year) for year in range(prev_year, prev_year - 4, -1)]
    for row in df_base.loc[df_base['data_all'] == 1].iloc[:].itertuples():
        if row.Index % 100 == 0:
            print(row.Index, row.symbol)
        # kgv_real = kgv_real + f.scrape_finanzen_kgv_real(row.isin, row.kgv_old_url, REL_YEARS_REAL)
        kgv_real += fin_handler.scrape_kgv_real(row.isin, row.kgv_old_url, REL_YEARS_REAL, row.name_finanzen)
        time.sleep(np.random.uniform(0.3, 0.8))
    df_kgv_real = pd.DataFrame(kgv_real)
    df_kgv_real['kgv'] = np.where(df_kgv_real['kgv'] == '-', np.nan, df_kgv_real['kgv'])
    df_kgv_real['kgv'] = df_kgv_real['kgv'].str.replace(".","").str.replace(",", ".").astype(float)
    len_org = df_kgv_real.shape[0]
    # drop duplicates (sometimes errors on finanzen.net)
    df_kgv_real.drop_duplicates(inplace=True)
    print(f"KGV hist: {len_org - df_kgv_real.shape[0]} duplicates dropped")
    df_kgv_real_wide = df_kgv_real.loc[~df_kgv_real['kgv'].isna()].pivot(index=['isin'], columns=['year'], values='kgv').reset_index()
    # df_kgv_real_wide.to_excel(PATH + FILE_KGV_REAL, index=False)
    print("code real_kgv finished successfully")
    # save results
    df_kgv_real_wide.to_excel(PATH + FILE_KGV, index=False)

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logging.info(f"{dt.datetime.now().strftime('%d.%m.%Y %H:%M:%S')} Exception extract_kgv_real main")
        logging.error(err, stack_info=True, exc_info=True)
        sys.exit(1)