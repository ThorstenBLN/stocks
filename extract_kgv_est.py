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
    FILE_KGV = "kgv_5y.xlsx"
    FILE_KGV_REAL = "kgv_5y_real.xlsx"

    # 1. load symbols
    df_base = pd.read_excel(PATH + FILE_SYMBOLS)

    # 2.3 scrape estimated KGV (ca. 20 min for 1000 symbols)
    kgv_est = []
    fin_handler = finhandler.Finhandler()
    # rel years are past year plus 3 more years
    prev_year = dt.datetime.now().year - 1
    REL_YEARS_EST = [str(year) + "e" for year in range(prev_year, prev_year + 4)]
    for row in df_base.loc[df_base['data_all'] == 1].iloc[:].itertuples():
        if row.Index % 100 == 0:
            print(row.Index, row.symbol)
        # kgv_est = kgv_est + f.scrape_finanzen_kgv_est(row.isin, row.kgv_est_url, REL_YEARS_EST)
        kgv_est += fin_handler.scrape_kgv_est(row.isin, row.kgv_est_url, REL_YEARS_EST, row.name_finanzen)
        time.sleep(np.random.uniform(0.125, 0.45))
    df_kgv_est = pd.DataFrame(kgv_est)
    df_kgv_est['kgv'] = np.where(df_kgv_est['kgv'] == "-", np.nan, df_kgv_est['kgv'])
    df_kgv_est['kgv'] = df_kgv_est['kgv'].str.replace(".","").str.replace(",", ".").astype(float)
    # drop duplicates (sometimes errors on finanzen.net)
    len_org = df_kgv_est.shape[0]
    df_kgv_est.drop_duplicates(inplace=True)
    print(f"KGV est: {len_org - df_kgv_est.shape[0]} duplicates dropped")
    df_kgv_est_wide = df_kgv_est.loc[~df_kgv_est['kgv'].isna()].pivot(index='isin', columns='year', values='kgv').reset_index()
    # df_kgv_est_wide.to_excel(PATH + FILE_KGV_EST, index=False)
    print("code est_kgv finished successfully")

    df_kgv_real_wide = pd.read_excel(PATH + FILE_KGV_REAL)

    # 2.4. add both data and calculate 5 years kgv
    df_kgv = df_kgv_real_wide.merge(df_kgv_est_wide, on='isin', how='outer')
    prev_year = dt.datetime.now().year - 1
    df_kgv['kgv_5y'] = np.where(df_kgv[str(prev_year)].notna(), df_kgv[[str(year) if year <= prev_year else str(year) + "e" for year in range(prev_year - 2, prev_year + 3)]].mean(axis=1, skipna=True), 
                                df_kgv[[str(year) if year < prev_year else str(year) + "e" for year in range(prev_year - 2, prev_year + 3)]].mean(axis=1, skipna=True))
    df_kgv['kgv_5y'] = df_kgv['kgv_5y'].astype('float')
    df_kgv['dwl_date_kgv'] = time.strftime("%Y%m%d")
    df_kgv.to_excel(PATH + FILE_KGV, index=False)

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logging.info(f"{dt.datetime.now().strftime('%d.%m.%Y %H:%M:%S')} Exception extract_kgv_est main")
        logging.error(err, stack_info=True, exc_info=True)
        print(err)
        sys.exit(1)