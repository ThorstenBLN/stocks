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

# functions
def get_hist_prices(data: pd.DataFrame, dates:dict):
    # get historic prices of the date (try except if there was no trade day)
    df_prices = pd.DataFrame()
    for per, date in dates.items():
        for i in range(4):
            check_date = (date - pd.DateOffset(days=i)).date()
            try:
                df_prices = pd.concat([df_prices, pd.DataFrame({'Date': check_date, 'Close':data.loc[data['Date'] == check_date]['Close'].values[0]}, index=[per])])            
                break
            except:
                # print(f"no data found at {check_date}")
                continue
            # price = df_hist.loc[df_hist['Date'] == date]['Close'].values[0]
    return df_prices

def yf_data_available(symbol):
    '''checks data availability of a certain symbol in yfinance'''
    try: 
        df = yf.Ticker(symbol).history(period="1d")
        if df.empty:
            return 0    
        return 1
    except:
        return 0

def get_levermann_data(row, df_dax_hist, df_dax_prices, dates, qrt_date, jv_date):
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
        df_prices = get_hist_prices(df_hist, dates)
        result_temp['data_date'] = df_hist['Date'].max()
    except:
        try:
            df_hist = dat.history(period=dat.get_history_metadata()['validRanges'][-1]).reset_index()
            df_hist['Date'] = df_hist['Date'].dt.date
            df_prices = get_hist_prices(df_hist, dates)
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

def add_levermann_score(df_data, na_penalty):
    '''add all levemannscores to the dataframe'''
    df_valid = df_data.clean_names(strip_underscores=True).copy()
    analyst_start = list(df_valid.columns).index('strongbuy')
    df_valid['n_rec'] = df_valid.iloc[:, analyst_start:analyst_start + 5].sum(axis=1)
    df_valid['analyst_metric'] = (df_valid['strongbuy'] + df_valid['buy'] + 2 * df_valid['hold'] + 3 * (df_valid['sell'] + df_valid['strongsell'])) / df_valid['n_rec']
    # score_start = len(df_valid.columns)
    df_valid['lev_ekr'] = np.where(df_valid['eigenkapital_rendite'].isna(), na_penalty, np.where(df_valid['eigenkapital_rendite'] > 0.2, 1, np.where(df_valid['eigenkapital_rendite'] >= 0.1, 0, -1)))
    df_valid['lev_ebitm'] = np.where(df_valid['ebit_marge'].isna(), na_penalty, np.where(df_valid['ebit_marge'] > 0.12, 1, np.where(df_valid['ebit_marge'] >= 0.06, 0, -1)))
    df_valid['lev_ekq'] = np.where(df_valid['ek_quote'].isna(), na_penalty, np.where(df_valid['ek_quote'] > 0.25, 1, np.where(df_valid['ek_quote'] >= 0.15, 0, -1)))
    mask_fin = df_valid['finance'] == 1
    df_valid['lev_ekq'] = np.where(df_valid['ek_quote'].isna(), na_penalty, np.where(mask_fin & (df_valid['ek_quote'] > 0.10), 1, np.where(mask_fin & (df_valid['ek_quote'] >= 0.05), 0, np.where(mask_fin & (df_valid['ek_quote'] < 0.05), -1, df_valid['lev_ekq']))))
    df_valid['lev_kgv5y'] = np.where(df_valid['kgv_5y'].isna(), na_penalty, np.where(df_valid['kgv_5y'] < 0, -1, np.where(df_valid['kgv_5y'] < 12, 1, np.where(df_valid['kgv_5y'] <= 16, 0, -1))))
    df_valid['lev_fkgv'] = np.where(df_valid['forward_kgv'].isna(), na_penalty, np.where(df_valid['forward_kgv'] < 0, -1, np.where(df_valid['forward_kgv'] < 12, 1, np.where(df_valid['forward_kgv'] <= 16, 0, -1))))
    df_valid['lev_anam'] = np.where(df_valid['analyst_metric'].isna(), na_penalty, np.where(df_valid['analyst_metric'] > 2.5, 1, np.where(df_valid['analyst_metric'] >= 1.5, 0, -1)))
    mask_sc  = (df_valid['cap_size'] == "small") & (df_valid['n_rec'] <= 5)
    df_valid['lev_anam'] = np.where(df_valid['analyst_metric'].isna(), na_penalty, np.where((df_valid['analyst_metric'] < 1.5) & mask_sc, 1, np.where((df_valid['analyst_metric'] <= 2.5)  & mask_sc, 0, np.where((df_valid['analyst_metric'] > 2.5) & mask_sc, -1, df_valid['lev_anam']))))
    df_valid['lev_rqrt'] = np.where(df_valid['reaktion_qrt'].isna(), na_penalty, np.where(df_valid['reaktion_qrt'] > 0.01, 1, np.where(df_valid['reaktion_qrt'] >= -0.01, 0, -1)))
    df_valid['lev_gewr'] = np.where(df_valid['gewinnrevision'].isna(), na_penalty, np.where(df_valid['gewinnrevision'] > 0.05, 1, np.where(df_valid['gewinnrevision'] >= -0.05, 0, -1)))
    df_valid['lev_kurs6m'] = np.where(df_valid['up_6m'].isna(), na_penalty, np.where(df_valid['up_6m'] > 0.05, 1, np.where(df_valid['up_6m'] >= -0.05, 0, -1)))
    df_valid['lev_kurs12m'] = np.where(df_valid['up_12m'].isna(), na_penalty, np.where(df_valid['up_12m'] > 0.05, 1, np.where(df_valid['up_12m'] >= -0.05, 0, -1)))
    df_valid['lev_kmom'] = np.where(df_valid['kursmomentum'].isna(), na_penalty, np.where(df_valid['kursmomentum'] > 0.02, 1, np.where(df_valid['kursmomentum'] >= -0.02, 0, -1)))
    df_valid['lev_dmr'] = np.where(df_valid['up_vs_dax_3m'].isna(), na_penalty, np.where(df_valid['up_vs_dax_3m'] > 0.02, 1, np.where(df_valid['up_vs_dax_3m'] >= -0.02, 0, -1)))
    df_valid['lev_gw'] = np.where(df_valid['cur_gewinnwachstum'].isna(), na_penalty, np.where(df_valid['cur_gewinnwachstum'] > 0.05, 1, np.where(df_valid['up_vs_dax_3m'] >= -0.05, 0, -1)))
    score_start = list(df_valid.columns).index('lev_ekr')
    df_valid['lev_score'] = df_valid.iloc[:, score_start:].sum(axis=1)
    return df_valid


def get_xetra_symbol_file():
    '''downloads the csv file from deutsche Börese homepage and stores it to data'''
    URL = "https://www.deutsche-boerse-cash-market.com/dbcm-de/instrumente-statistiken/alle-handelbaren-instrumente/boersefrankfurt"
    page = requests.get(URL)
    if page.status_code == 200:
        soup = BeautifulSoup(page.content, 'html.parser', from_encoding="utf-8")
        link = soup.find('a', href=lambda href: href and "allTradableInstruments.csv" in href)
        if link:
            response =  requests.get("https://www.deutsche-boerse-cash-market.com/" + link['href'])
            with open("./data/symbols_xetra.csv", "wb") as file:
                file.write(response.content)
        else:
            print("no file found")

def update_exr(exr_dict, cur_currency):
    '''checks if currency exchangerate is present or adds it if not'''
    if cur_currency not in exr_dict.keys():
        data_exr = yf.Ticker(f'EUR{cur_currency.upper()}=X').info
        exr_dict[cur_currency] = data_exr['regularMarketPrice']
    return exr_dict

def yf_xetra_data_available(index, isin_code):
    '''checks avaiability of isin in yfinance and returns the symbol'''
    try: 
        data = yf.Ticker(isin_code)# , session=session)
        return data.info['symbol']    
    except Exception as err:
        print("0", index, isin_code, err)
        return np.nan
    
def update_bank_and_taxes(df_depot, row, tax_rate, fee):
    '''calculates tax of sales and deducts it from account or adds it to tax_cum (taxshield for losses)'''
    gain = (row.price_cur_eur - row.price_buy_eur) * row.amount
    taxes_cum = df_depot.at[0, 'tax_cum'] + gain * tax_rate
    # if cumultative taxes < 0: pay taxes and set them back to 0
    if taxes_cum > 0:
        # else add sales value to bank account
        df_depot.at[0, "value_eur"] += row.value_eur - taxes_cum - fee
        df_depot['tax_cum'] = 0
    # if sold with loss add to taxes as taxshield
    else:
        df_depot.at[0, "value_eur"] += row.value_eur - fee
        df_depot['tax_cum'] = taxes_cum
    return df_depot, taxes_cum

def create_sales_info(df_sales, row, taxes_cum, fee):
    '''create a dataframe row with all important sales info'''
    df_temp = df_sales.loc[df_sales['isin'] == row.isin].copy()
    cols = list(df_temp.columns)
    df_temp["type"] = "sell"   
    df_temp["taxes_paid"] = max(0, taxes_cum)
    df_temp["fee"] = fee
    df_temp[['type'] + cols + ['taxes_paid', 'fee']]
    return df_temp

def buy_stock(row, value, cur_time, cur_exr, tax_cum, stop_loss_pc, fee):
    '''create a dataframe row with all important pruchase info'''
    cur_info = yf.Ticker(row.symbol).info
    cur_price = cur_info['regularMarketPrice']
    cur_cur = cur_info['currency']
    cur_exr = update_exr(cur_exr, cur_cur)
    amount = value // (cur_price / cur_exr[cur_cur])
    df_temp = pd.DataFrame({"type":"buy", 'isin':row.isin, "symbol":row.symbol, 'symbol_finanzen':row.symbol_finanzen, 
                            'name': row.name,'buy_date':cur_time, 
                            'price_buy':cur_price, 'cur':cur_cur, 'exr_hist':cur_exr[cur_cur], 
                            'price_buy_eur':cur_price / cur_exr[cur_cur], 'amount':amount,'lev_buy':row.lev_score, 
                            'mean_top_buy':row.mean_top, 'score_tot_buy': row.score_tot, 'cur_date':cur_time, 'price_cur':cur_price, 'cur2':cur_cur, 
                            'exr_cur':cur_exr[cur_cur], "price_cur_eur":cur_price/cur_exr[cur_cur], 
                            'value_org':cur_price * amount, 
                            'value_eur':cur_price / cur_exr[cur_cur] * amount , 
                            'stop_loss_eur':cur_price / cur_exr[cur_cur] * stop_loss_pc, 
                            'rendite_org':0.0, 'rendite_eur':0.0, 'lev_score': row.lev_score,
                            'mean_top':row.mean_top, 'score_tot': row.score_tot, 
                            'tax_cum':tax_cum, 'fee':fee}, index=[0]) 
    return df_temp

def add_to_message(text, df_temp):
    if text in ['buy rules', 'sell rules', "buy opt", "sell opt"]:
        return f"{text}:\nISIN: {df_temp['isin'].values[0]}\n{df_temp['name'].values[0]}\nValue: {np.round(df_temp['value_eur'].values[0], 2)} EUR\n\n"
    

def send_telegram_msg(msg, token, chat_id):
    url_send = f'https://api.telegram.org/bot{token}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': msg
    }
    response = requests.post(url_send, data=payload)
    if response.status_code == 200:
        return 'Message sent successfully!'
    else:
        return 'Message not sent!'
    
def update_depot(df_depot, row, cur_time, cur_exr):
    '''updates the depot stocks with current values'''
    cur_info = yf.Ticker(row.symbol).info
    cur_price = cur_info['regularMarketPrice']
    cur_currency = cur_info['currency']
    df_depot.at[row.Index, "cur_date"] = cur_time # pd.time.strftime("%Y-%m-%d")
    df_depot.at[row.Index, "price_cur"] = cur_price
    df_depot.at[row.Index, "cur2"] = cur_currency
    cur_exr = update_exr(cur_exr, cur_currency)
    df_depot.at[row.Index, "exr_cur"] = cur_exr[cur_currency]
    df_depot.at[row.Index, "price_cur_eur"] = cur_price / cur_exr[cur_currency]
    df_depot.at[row.Index, "value_org"] = cur_price * row.amount
    df_depot.at[row.Index, "value_eur"] = cur_price * row.amount / cur_exr[cur_currency]
    df_depot.at[row.Index, "rendite_org"] = cur_price / row.price_buy - 1
    df_depot.at[row.Index, "rendite_eur"] = (cur_price/cur_exr[cur_currency]) / (row.price_buy/row.exr_hist) - 1

def define_invest_value(bank_funds, INVEST_VALUE, MIN_INVEST_VALUE, TRADING_FEE):
    '''calculates the investment value or None of not enough funds'''
    if bank_funds >= INVEST_VALUE:
        value = INVEST_VALUE - TRADING_FEE
    elif bank_funds >= MIN_INVEST_VALUE:
        value = bank_funds - TRADING_FEE
    else:
        value = None 
    return value

def get_historic_data(row, per="5y"):
    '''get at max 5 years data from all stocks for lstm training'''
    dat = yf.Ticker(row.symbol)
    try:
        df_hist = dat.history(period=per).reset_index()
    except:
        try:
            df_hist = dat.history(period=dat.get_history_metadata()['validRanges'][-1]).reset_index()
        except:
            return pd.DataFrame()
    df_hist['isin'] = row.isin
    df_hist['symbol'] = row.symbol
    return df_hist

def get_pred_arrays(indices_dict, np_all, win_len, norm_col):
    '''iterate over grouped indices. last column must be y column
    creates x and y arrays. normalizes x array by last value of norm-col'''
    pred_isin, pred_ind, pred_start, pred_end = [], [], [], []
    X_pred = []
    for i, entry in enumerate(indices_dict.items()):
        X_pred_isin =  []
        for start in entry[1][:-win_len + 1]:
            last = start + win_len - 1 # correct window
            x = np_all[start:last + 1, :].copy()
            # normalize data
            x[:, :norm_col[1]] = x[:, :norm_col[1]] / np_all[last, norm_col[0]]
            x[:, norm_col[1]:norm_col[2]] = x[:, norm_col[1]:norm_col[2]] / np_all[last, norm_col[1]]
            x[:, norm_col[2]:] = x[:, norm_col[2]:] / np_all[last, norm_col[2]]
            X_pred_isin.append(x)
            pred_isin.append(entry[0])
            pred_start.append(start.item())
            pred_end.append(last.item())
        X_pred.extend(np.array(X_pred_isin))
    print(np.array(X_pred).shape)
    return np.array(X_pred), pd.DataFrame({'isin':pred_isin, 'start_df': pred_start, 'end_df':pred_end})