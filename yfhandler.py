import requests
import random
import time
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import yfinance as yf
import re
import janitor
import os

class YfHandler():
    def __init__(self):
        self.data = None

    def get_hist_prices(self, data: pd.DataFrame, dates:dict):
        ''' returns dataframe with historic prices of the input dates 
        if no trade at the specified dates tries up to 3 days earlier'''
        df_prices = pd.DataFrame()
        for per, date in dates.items():
            for i in range(4):
                check_date = (date - pd.DateOffset(days=i)).date()
                try:
                    df_prices = pd.concat([df_prices, pd.DataFrame({'Date': check_date, 'Close':data.loc[data['Date'] == check_date]['Close'].values[0]}, index=[per])])            
                    break
                except:
                    continue
        return df_prices
    
    def yf_xetra_data_available(self, index, isin_code):
        '''checks avaiability of isin in yfinance and returns the symbol'''
        try: 
            data = yf.Ticker(isin_code)# , session=session)
            return data.info['symbol']    
        except Exception as err:
            print("0", index, isin_code, err)
            return np.nan
        
    def update_exr(self, exr_dict, cur_currency):
        '''checks if currency exchangerate is present or adds it if not'''
        if cur_currency not in exr_dict.keys():
            data_exr = yf.Ticker(f'EUR{cur_currency.upper()}=X').info
            exr_dict[cur_currency] = data_exr['regularMarketPrice']
        return exr_dict
    
    def get_levermann_data(self, row, df_dax_hist, df_dax_prices, dates, qrt_date, jv_date):
        '''get all data needed to calculate the levermann formula'''
        FINANCE = ['Capital Markets','Credit Services','Financial Conglomerates', 'Financial Data & Stock Exchanges', 'Banks - Diversified', 'Banks - Regional', 'Mortgage Finance']
        MAX_QRT_DAY_DISTANCE = 200
        FIELDS = ['data_date', 'industry', 'finance', 'cap_size', 'market_cap', 'eigenkapital_rendite', 'ebit_marge', 'ek_quote', 'forward_kgv','reaktion_qrt', 'gewinnrevision', 
                'up_6m', 'up_12m', 'kursmomentum', 'up_vs_dax_3m', 'up_vs_dax_6m', 'cur_gewinnwachstum', 'strongBuy', 'buy', 'hold', 'sell', 'strongSell']
        # predefine results dict
        result_temp = {'isin':row.isin, 'symbol':row.symbol, 'symbol_finanzen':row.symbol_finanzen, 'name':row.name, 'download_date':time.strftime("%Y%m%d")}
        # add dates and timediff and check which data is relevant for reaktion auf geschäftszahlen
        result_temp['rel_financials_date'] = None
        if jv_date.empty:
            result_temp['jv_date'] = np.nan
        else:
            result_temp['jv_date'] = jv_date.values[0]
            result_temp['rel_financials_date'] = result_temp['jv_date']
        if qrt_date.empty:
            result_temp['qrt_date'] = np.nan
        else:
            result_temp['qrt_date'] = qrt_date.values[0]
            if not result_temp['rel_financials_date']:
                result_temp['rel_financials_date'] = result_temp['qrt_date']
            else:
                result_temp['rel_financials_date'] = max(result_temp['qrt_date'], result_temp['jv_date'])
        if  result_temp['rel_financials_date']:
            result_temp['days_passed'] = (pd.to_datetime("today") - pd.to_datetime(result_temp['rel_financials_date'])).days
        # setup rest of dict
        for key in FIELDS:
            result_temp[key] = np.nan
        # get stock data
        dat = yf.Ticker(row.symbol)
        # try to get the historic data for 2 years. if not available take max timeframe
        try:
            df_hist = dat.history(period="2y").reset_index()
            df_hist['Date'] = df_hist['Date'].dt.date
            df_prices = self.get_hist_prices(df_hist, dates)
            result_temp['data_date'] = df_hist['Date'].max()
        except:
            try:
                df_hist = dat.history(period=dat.get_history_metadata()['validRanges'][-1]).reset_index()
                df_hist['Date'] = df_hist['Date'].dt.date
                df_prices = self.get_hist_prices(df_hist, dates)
                result_temp['data_date'] = df_hist['Date'].max()
            except:
                df_hist = pd.DataFrame()
                df_prices = pd.DataFrame()
                result_temp['data_date'] = np.nan
        try:
            df_bs = dat.balance_sheet
            df_is = dat.income_stmt
            df_eps = dat.eps_trend
        except:
            df_bs = pd.DataFrame()
            df_is = pd.DataFrame()
            df_eps = pd.DataFrame()    
        # error handling
        if any([df.empty or df is None for df in [df_hist, df_prices, df_bs, df_is, df_eps]]): 
            print("no valid data -> skipping")
            return result_temp
        try:
            result_temp['industry'] = dat.info['industry']
            result_temp['market_cap'] = int(dat.info['marketCap'])
        except:
            result_temp['industry'] = np.nan
            result_temp['market_cap'] = 0
        result_temp['cap_size'] = np.where(result_temp['market_cap'] < 2000000000, "small", np.where(result_temp['market_cap'] < 5000000000, "mid", "big"))
        result_temp['finance'] = 0
        if result_temp['industry'] in FINANCE:
            result_temp['finance'] = 1
        # 2. Eigenkapitalrendite
        try:
            result_temp['eigenkapital_rendite'] = df_is.loc['Net Income'].iloc[0] / df_bs.loc['Total Equity Gross Minority Interest'].iloc[0]
        except:
            result_temp['eigenkapital_rendite'] = np.nan
        # 3. EBIT-Marge
        try:
            result_temp['ebit_marge'] = df_is.at['EBIT', df_is.columns[0]] / df_is.at['Total Revenue', df_is.columns[0]]
        except:
            result_temp['ebit_marge'] = np.nan
        # 4. EK-Quote
        try:
            result_temp['ek_quote'] = df_bs.at['Total Equity Gross Minority Interest', df_bs.columns[0]] / df_bs.at['Total Assets', df_bs.columns[0]]
        except:
            result_temp['ek_quote'] = np.nan
    
        # 6. forward P/E (KGV)
        try:
            result_temp['forward_kgv'] = dat.info['forwardPE'] # nicht 100% sicher
            if result_temp['forward_kgv'] == "Infinity":
                result_temp['forward_kgv'] = np.inf
        except:
            result_temp['forward_kgv'] = np.nan
        # 7. Analystenmeinung
        try:
            df_analyst = dat.recommendations
            if not df_analyst.empty:
                for key in df_analyst.columns[1:]:
                    result_temp[key] =  df_analyst.at[0, key]
        except:
            pass
        # 8. Reaktion auf Geschäftszahlen 
        # check which date to use (JV oder QRT)
        if not result_temp['rel_financials_date']:
            print("no_qrt_date")
            result_temp['reaktion_qrt'] = np.nan
            result_temp['rel_financials_date'] = np.nan
        # calculate the time difference in order to chose day before and after
        # +1 means 1 day after the event -1 one day before event, 0 = event day
        else:
            df_hist['date_diff'] = (pd.to_datetime(df_hist['Date']) - pd.to_datetime(result_temp['rel_financials_date'])).dt.days
            df_dax_hist['date_diff'] = (pd.to_datetime(df_dax_hist['Date']) - pd.to_datetime(result_temp['rel_financials_date'])).dt.days
            # check if there is a the window of +/- 1 day in the data 
            # (1. qrt date way too old, 2. all data newer or same day as qrt date (so no day before), 3. qrt date newer than any data or same day
            if df_hist['date_diff'].max() > MAX_QRT_DAY_DISTANCE or df_hist['date_diff'].min() >= 0 or df_hist['date_diff'].max() < 0:
                result_temp['reaktion_qrt'] = np.nan
                print("no valid qrt_date / or data before the qrt date")
            else:
                # print(df_hist['date_diff'].max())
                # calculate the values for dax
                min_later_dax = df_dax_hist.loc[df_dax_hist['date_diff'] > 0]['date_diff'].min()
                if df_dax_hist['date_diff'].max() == 0: # on the day of the qrt release, take this day
                    min_later_dax = df_dax_hist.loc[df_dax_hist['date_diff'] == 0]['date_diff'].min()
                price_dax_next = df_dax_hist.loc[df_dax_hist['date_diff'] == min_later_dax]['Close'].values[0]
                min_before_dax = df_dax_hist.loc[df_dax_hist['date_diff'] < 0]['date_diff'].max()
                price_dax_before = df_dax_hist.loc[df_dax_hist['date_diff'] == min_before_dax]['Close'].values[0]
                # caluclate the value for our stock
                min_later = df_hist.loc[df_hist['date_diff'] > 0]['date_diff'].min()
                if df_hist['date_diff'].max() == 0: # on the day of the qrt release, take this day
                    min_later = df_hist.loc[df_hist['date_diff'] == 0]['date_diff'].min()
                price_next = df_hist.loc[df_hist['date_diff'] == min_later]['Close'].values[0]
                min_before = df_hist.loc[df_hist['date_diff'] < 0]['date_diff'].max()
                price_before = df_hist.loc[df_hist['date_diff'] == min_before]['Close'].values[0]
                result_temp['reaktion_qrt']  = (price_next / price_before) / (price_dax_next / price_dax_before) - 1
        # 9 Gewinnrevision (erwartung des EPS heute vs. vor 30 Tagen)
        try:
            result_temp['gewinnrevision'] = df_eps.at['0y', 'current'] / df_eps.at['0y', '7daysAgo'] - 1
        except:
            result_temp['gewinnrevision'] = np.nan 

        # 10. Kurs heute vs. 6 M
        try:
            result_temp['up_6m'] = (df_prices.at['cur', 'Close'] / df_prices.at['6m', 'Close']) - 1
        except:
            result_temp['up_6m'] = np.nan

        # 11. Kurs heute vs. 12 M
        try:
            result_temp['up_12m'] = (df_prices.at['cur', 'Close'] / df_prices.at['12m', 'Close']) - 1
        except:
            result_temp['up_12m'] = np.nan
        
        # 12. Kursmomentum
        try:
            result_temp['kursmomentum'] = (df_prices.at['cur', 'Close'] / df_prices.at['6m', 'Close']) / (df_prices.at['cur', 'Close'] / df_prices.at['12m', 'Close']) - 1
        except:
            result_temp['kursmomentum'] = np.nan

        # 13 Entwicklung letzte 3 Monate ggü. Index (DAX)  6Mx / 6Mp – 1
        try:
            result_temp['up_vs_dax_3m'] = (df_prices.at['cur', 'Close'] / df_prices.at['3m', 'Close']) / (df_dax_prices.at['cur', 'Close'] / df_dax_prices.at['3m', 'Close']) - 1
        except:
            result_temp['up_vs_dax_3m'] = np.nan
        try:
            result_temp['up_vs_dax_6m'] = (df_prices.at['cur', 'Close'] / df_prices.at['6m', 'Close']) / (df_dax_prices.at['cur', 'Close'] / df_dax_prices.at['6m', 'Close']) - 1
        except:
            result_temp['up_vs_dax_6m'] = np.nan

        # 14. gewinn-wachstum    
        try:
            result_temp['cur_gewinnwachstum'] = df_eps.at['+1y', 'current'] / df_eps.at['0y', 'current']  - 1 
        except:
            result_temp['cur_gewinnwachstum'] = np.nan
        # create and return datafrane
        return result_temp