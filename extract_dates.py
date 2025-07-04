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
import traceback

def main():
    warnings.simplefilter('ignore', 'FutureWarning')
    logging.basicConfig(filename='./data/data_pipeling.log', level=logging.INFO)

    PATH = "./data/"
    FILE_SYMBOLS = "symbols.xlsx"
    FILE_DATES = "dates.xlsx"

    # 1. load symbols
    df_base = pd.read_excel(PATH + FILE_SYMBOLS)

    # 2. finanzen.net: scrape data ############################################################
    fin_handler = finhandler.Finhandler()
    # 2.1 scrape termine scrapet the vergangenen Termine (ca. 20 min for 1000 symbols)
    # df_dates = f.scrape_dates(df_base.loc[df_base['data_all'] == 1].iloc[:])
    df_dates = fin_handler.get_all_dates(df_base.loc[df_base['data_all'] == 1].iloc[:])
    df_dates.to_excel(PATH + FILE_DATES, index=False)

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        traceback.print_exc(limit=None, file=None, chain=True)
        sys.exit(1)
