import pandas as pd
import numpy as np
import time
import openpyxl
import datetime as dt
import warnings
import functions as f
import os
import requests
import logging
import sys
import finhandler
import traceback

def main():
    time_1 = time.time()
    warnings.simplefilter('ignore', 'FutureWarning')
    logging.basicConfig(filename='./data/data_pipeling.log', level=logging.INFO)

    PATH = "./data/"
    FILE_SYM = "symbols.xlsx"
    FILE_GER = "symbols_xetra.csv"
    FILE_EXCLUDE = "exclude_isin.csv"

    if not os.path.exists(PATH):
        os.makedirs(PATH)

    # 0. try to load current used and deleted symbols
    if os.path.exists(PATH + FILE_SYM):
        df_used = pd.read_excel(PATH + FILE_SYM).clean_names(strip_underscores = True)
    else:
        df_used = None

    # 1. XETRA STOCKS UPDATE #####################################################################
    # 1.1. download Xetra symbols
    # get all xetra symbols
    f.get_xetra_symbol_file()
    rel_cols = list(range(19)) + [61, 62, 66, 132]
    df_xetra = pd.read_csv(PATH + FILE_GER, skiprows=2, sep=";", header=0, engine="python") \
                        .clean_names(strip_underscores=True).iloc[:, rel_cols]
    mask = (df_xetra['instrument_status'] == "Active") & (df_xetra['instrument_type'] == "CS") #  & (df_symbols_ger['ccp_eligible_code'] == "Y")
    df_xetra = df_xetra.loc[mask]
    df_xetra.rename(columns={'instrument':'name'}, inplace=True)
    df_xetra['price'] = 0
    df_xetra['type'] = "stock"
    df_xetra['symbol'] = np.nan
    df_xetra['status'] = "xetra_ver"
    df_xetra['data_yf'] = 0
    time_2 = time.time()
    print(f"loading xetra file: {np.round((time_2 - time_1)/60, 2).item()} minutes")

    COLUMNS_XETRA = ['symbol', 'isin', 'name', 'price', 'type', 'status','data_yf']
    df_xetra = df_xetra[COLUMNS_XETRA].reset_index(drop=True)[:] 
    # 1.2. check for new xetra symbols and for the current and deleted ones to recheck
    if df_used is not None:
        # 1. define current xetra symbols to delete if not in xetra file anymore
        df_xetra['exists_xetra'] = 1
        df_used = df_used.merge(df_xetra[['isin', 'exists_xetra']], on='isin', how='left')
        df_used['exists_xetra'] = np.where((df_used['exists_xetra'].isna()), 0, 1) # set the not anymore existing ones to 0
        # 2. check for new symbols in xetra file
        df_new = df_xetra.drop(columns=['exists_xetra']).merge(df_used[['isin', 'exists_xetra']], on='isin', how='left')
        df_new = df_new.loc[df_new['exists_xetra'].isna()] # is 0 or 1 if in used file and na if not in used file
        # 3. define the stocks to recheck (only stocks, which are still in the xetra csv)
        df_xetra_check = df_new.drop(columns=['exists_xetra']).reset_index(drop=True)
        # 4. delete unnecessary data from df_used and df_del
        df_used = df_used.loc[df_used['exists_xetra'] == 1].drop(columns=['exists_xetra']) # keep only those which are still in xetra list
    else:
        df_xetra_check = df_xetra.copy().reset_index(drop=True)
        
    # 1.3. check if xetra data in yfinance (ca. 18 min for 1000 symb) ###########################################
    symbols_yf = []
    # df_xetra_check = df_xetra_check.iloc[:10].copy()
    for row in df_xetra_check.iloc[:].itertuples(): #6109: 10005
        if row.Index % 100 == 0:
            print((row.Index, row.isin))
        symbols_yf.append(f.yf_xetra_data_available(row.Index, row.isin))
        time.sleep(np.random.uniform(0.3, 0.8))
    print("all data collected")
    df_xetra_check['symbol'] = symbols_yf
    df_xetra_check['data_yf'] = np.where(df_xetra_check['symbol'].isna(), 0, 1)
    time_1 = time.time()
    print(f"check yf data: {np.round((time_1 - time_2)/60, 2).item()} minutes")


    # 3. check valid symbols at finanzen.net (ca. 1h for each 1000 sybols)
    fin_handler = finhandler.Finhandler()
    # 3.1 get all relevnt links 
    fin_links = []
    for row in df_xetra_check.loc[df_xetra_check['data_yf'] == 1].iloc[:].itertuples():
        if row.Index % 100 == 0:
            print(row.Index, row.isin)
        fin_links.append(fin_handler.get_links(row.isin, row.name))
        time.sleep(np.random.uniform(0.3, 0.8))
    df_fin_links = pd.DataFrame(fin_links)
    df_fin_links.rename(columns={'symbol':'isin'}, inplace=True)
    # add links and check if symbols are identical 
    df_fin_links['kgv_old_url'] = fin_handler.base_url + "bilanz_guv/" + df_fin_links['name_finanzen'].astype(str)
    df_fin_links['kgv_est_url'] = fin_handler.base_url + "schaetzungen/" + df_fin_links['name_finanzen'].astype(str)
    time_2 = time.time()
    print(f"load links: {np.round((time_2 - time_1)/60, 2).item()} minutes")

    # 4. merge all data and save final list
    df_check_final = df_xetra_check.merge(df_fin_links[['isin', 'name_finanzen', 'symbol_finanzen', 'stock_url', 'termine_url', 'kgv_old_url', 'kgv_est_url']], on='isin', how='left')
    mask = (df_check_final['data_yf'] == 1) & (df_check_final['name_finanzen'].notna())
    df_check_final['data_all'] = np.where(mask, 1, 0)
    # save final files (updated symbol file and deleted file)
    if df_used is not None:
        # combine the new relevant xetra data and the old relevant data
        df_used_final = pd.concat([df_check_final.loc[df_check_final['data_all'] == 1], df_used]).drop_duplicates(subset='isin')
    else:
        # use only new data
        df_used_final = df_check_final.loc[df_check_final['data_all'] == 1].copy() 
    # exclude lstm exclusions
    df_exclude = pd.read_csv(PATH + FILE_EXCLUDE)
    exclude_cols = [col for col in df_used_final.columns if "exclude" in col]
    if len(exclude_cols) > 0:
        df_used_final.drop(columns=exclude_cols, inplace=True)
    df_used_final = df_used_final.merge(df_exclude, on='isin', how='left')
    df_used_final['exclude_lstm'] = df_used_final['exclude_lstm'].fillna(0)
    df_used_final.loc[(df_used_final['data_all'] == 1) & (df_used_final['exclude_lstm'] == 0)].to_excel(PATH + FILE_SYM, index=False)
    time_1 = time.time()
    print(f"save data: {np.round((time_1 - time_2)/60, 2).item()} minutes")

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        # logging.info(f"{dt.datetime.now().strftime('%d.%m.%Y %H:%M:%S')} Exception extract_isin main")
        # logging.error(err, stack_info=True, exc_info=True)
        traceback.print_exc(limit=None, file=None, chain=True)
        sys.exit(1)