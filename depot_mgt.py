import openpyxl
import pandas as pd
import numpy as np
import yfinance as yf
import warnings
import os
import time
import functions as f

warnings.simplefilter('ignore', 'FutureWarning')

PATH = "./data/"
FILE_RESULT_DAY  = "result.xlsx"
FILE_DEPOT = "depot.xlsx"
FILE_DEPOT_HIST = "depot_hist.xlsx"
FILE_TRANSACTIONS = "transactions.xlsx"

L_SCORE_BUY = 9
MIN_L_SCORE = 7
STOP_LOSS_PC = 0.85
THRES_LEV_BETTER = 3
INVEST_VALUE = 1500
MIN_INVEST_VALUE = 1000
TAX = 0.25

# 0. load relevant files
df_result = pd.read_excel(PATH + FILE_RESULT_DAY)
if not os.path.exists(PATH + FILE_DEPOT):
    # initialize bank account
    df_depot = pd.DataFrame({'isin':'bank', "symbol":'bank', 'symbol_finanzen':'bank','name':'account', 'buy_date':'2025-03-16',
                             'price_buy':1.00, 'cur':'EUR', 'exr_hist':1, 'price_buy_eur':1, 'amount':1,
                               'cur_date':'2025-03-17', 'price_cur':1.00, 'cur2':'EUR', 'exr_cur':1, 
                               "price_cur_eur":1, 'value_org':0, 'value_eur':10000, 'stop_loss_eur':0.00,
                                'rendite_org':0.00001, 'rendite_eur':0.00001, 'lev_score': 100.00}, index=[0])
else:
    df_depot = pd.read_excel(PATH + FILE_DEPOT)
if not os.path.exists(PATH + FILE_TRANSACTIONS):
    df_transact = pd.DataFrame()
else:
    df_transact = pd.read_excel(PATH + FILE_TRANSACTIONS)
if not os.path.exists(PATH + FILE_DEPOT_HIST):
    df_depot_hist = pd.DataFrame()
else:
    df_depot_hist = pd.read_excel(PATH + FILE_DEPOT_HIST)

# 1. update the current values of the stocks
cur_exr = {'EUR':1}
df_depot.reset_index(drop=True, inplace=True)
cur_time = pd.Timestamp.now() 
mask_bank = df_depot['isin'] == 'bank'
for row in df_depot.loc[~mask_bank].itertuples():
    try:
        cur_info = yf.Ticker(row.symbol).info
        cur_price = cur_info['regularMarketPrice']
        cur_currency = cur_info['currency']
        df_depot.at[row.Index, "cur_date"] = cur_time # pd.time.strftime("%Y-%m-%d")
        df_depot.at[row.Index, "price_cur"] = cur_price
        df_depot.at[row.Index, "cur2"] = cur_currency
        cur_exr = f.update_exr(cur_exr, cur_currency)
        df_depot.at[row.Index, "exr_cur"] = cur_exr[cur_currency]
        df_depot.at[row.Index, "price_cur_eur"] = cur_price / cur_exr[cur_currency]
        df_depot.at[row.Index, "value_org"] = cur_price * row.amount
        df_depot.at[row.Index, "value_eur"] = cur_price * row.amount / cur_exr[cur_currency]
        df_depot.at[row.Index, "rendite_org"] = cur_price / row.price_buy - 1
        df_depot.at[row.Index, "rendite_eur"] = (cur_price/cur_exr[cur_currency]) / (row.price_buy/row.exr_hist) - 1
    except Exception as err:
        print("0", row.symbol, err)
df_depot = df_depot.drop(columns='lev_score').merge(df_result[['isin', 'lev_score']], on='isin', how='left')
df_depot.at[0, "cur_date"] = cur_time # pd.time.strftime("%Y-%m-%d")
df_depot.at[0, 'lev_score'] = 100

# 2. buy/sell stocks 
# 2.1 create purchase options df
df_pur_opt = df_result.loc[df_result['lev_score'] >= L_SCORE_BUY].sort_values(['lev_score', 'market_cap'], ascending=[False, False]).copy()
df_pur_opt['in_dpt'] = np.where(df_pur_opt['isin'].isin(df_depot['isin'].unique()), 1, 0)
df_pur_opt['sold'] = 0
# 2.1 sell based on fixed values
mask_1 = df_depot['lev_score'] <= MIN_L_SCORE
mask_2 = df_depot['price_cur_eur'] <= df_depot['stop_loss_eur']
df_sales = df_depot.loc[mask_1 | mask_2].copy().reset_index()
for row in df_sales.itertuples():
    # do not sales a stock you would directly rebuy:
    df_pur_opt.at[df_pur_opt['isin'] == row.isin, 'in_dpt'] = 0
    if df_pur_opt.loc[df_pur_opt['in_dpt'] == 0].head(1)['isin'] == row.isin:
        df_pur_opt.at[df_pur_opt['isin'] == row.isin, 'in_dpt'] = 1
        print("no rebuy sales!")
        continue
    # else sell the stock and add money to bank (consider taxes at rendite_eur column of bank)
    gain = (row.price_cur_eur - row.price_buy_eur) * row.amount
    taxes_cum = df_depot.at[0, "rendite_eur"] + gain * TAX 
    # if cumultative taxes < 0: pay taxes and set them back to 0
    if taxes_cum > 0:
        # else add sales value to bank account
        df_depot.at[0, "value_eur"] += row.value_eur - taxes_cum   
        df_depot.at[0, "rendite_eur"] = 0
    # if sold with loss add to taxes as taxshield
    else:
        df_depot.at[0, "value_eur"] += row.value_eur
        df_depot.at[0, "rendite_eur"] = taxes_cum

    # add values to transitions
    df_temp = df_sales.iloc[row.index]
    cols = list(df_temp.columns)
    df_temp["type"] = "sell"
    df_temp["taxes_paid"] = max(0, taxes_cum)
    df_transact = pd.concat([df_transact, df_temp[['type'] + cols + ['taxes_paid']]])
    # delete stocks from depot
    df_depot = df_depot.loc[df_depot['isin'] != row.isin].reset_index(drop=True)

# 2.2 buy with bank money
# buy the best stocks from bank
mask_not_in_depot = df_pur_opt['in_dpt'] == 0
for row in df_pur_opt.loc[mask_not_in_depot].itertuples():
    mask_bank = df_depot['isin'] == 'bank'
    # buy all for invest value and last one for min value
    if df_depot.loc[mask_bank]['value_eur'].values[0] >= INVEST_VALUE:
        VALUE = INVEST_VALUE
    elif df_depot.loc[mask_bank]['value_eur'].values[0] >= MIN_INVEST_VALUE:
        VALUE = df_depot.loc[mask_bank]['value_eur'].values[0]
    else:
        break
    try:
        # perform purchase and add to all files 
        cur_info = yf.Ticker(row.symbol).info
        cur_price = cur_info['regularMarketPrice']
        cur_cur = cur_info['currency']
        cur_exr = f.update_exr(cur_exr, cur_cur)
        amount = VALUE // (cur_price / cur_exr[cur_cur])
        df_temp = pd.DataFrame({"type":"buy", 'isin':row.isin, "symbol":row.symbol, 'symbol_finanzen':row.symbol_finanzen, 'name': row.name,'buy_date':cur_time, 
                                'price_buy':cur_price, 'cur':cur_cur, 'exr_hist':cur_exr[cur_cur], 
                                'price_buy_eur':cur_price / cur_exr[cur_cur], 'amount':amount, 
                                'cur_date':cur_time, 'price_cur':cur_price, 'cur2':cur_cur, 
                                'exr_cur':cur_exr[cur_cur], "price_cur_eur":cur_price/cur_exr[cur_cur], 
                                'value_org':cur_price * amount, 
                                'value_eur':cur_price / cur_exr[cur_cur] * amount , 
                                'stop_loss_eur':cur_price / cur_exr[cur_cur] * STOP_LOSS_PC, 
                                'rendite_org':0.0, 'rendite_eur':0.0, 'lev_score': row.lev_score}, index=[0]) 
        df_transact = pd.concat([df_transact, df_temp]).reset_index(drop=True)
        df_depot = pd.concat([df_depot, df_temp.drop(columns=['type'])]).reset_index(drop=True)
        # reduce bank account value by purchase volume
        df_depot.at[0, 'value_eur'] = df_depot.at[0, "value_eur"] - cur_price / cur_exr[cur_cur] * amount
        # set in depot variable to 1
        df_pur_opt.at[row.Index, 'in_dpt'] = 1
    except Exception as err:
        print("1", row.symbol, err)
    
# 2.3. shift stocks to better options
# get possible stocks
mask_cur = df_result['isin'].isin(df_depot['isin'].unique())
mask_shift_stocks = df_result['lev_score'] >= df_depot['lev_score'].min() + THRES_LEV_BETTER
df_buy_opt = df_result.loc[mask_shift_stocks & ~mask_cur].sort_values(['lev_score', 'market_cap'], ascending=[False, False]).reset_index(drop=True)
# prepare depot dataframe
mask_bank = df_depot['isin'] == 'bank'
df_sales = df_depot.loc[~mask_bank].sort_values('lev_score').copy().reset_index(drop=True)
isin_sold = []
for row in df_sales.itertuples():
    if df_buy_opt.shape[0]  < row.Index + 1:
        break
    if row.lev_score + THRES_LEV_BETTER < df_buy_opt.at[row.Index, 'lev_score']: 
        try: 
            # add sale value to bank
            df_depot.at[0, "value_eur"] = df_depot.at[0, "value_eur"] + df_sales.iloc[row.Index]['value_eur']
            # add sale values to transitions
            cols = list(df_sales.columns)
            df_temp = df_sales.iloc[row.Index].copy()
            df_temp["type"] = "sell"
            df_transact = pd.concat([df_transact, df_temp[['type'] + cols]])
            isin_sold.append(row.isin)
            # buy other article
            cur_info = yf.Ticker(df_buy_opt.at[row.Index, 'symbol']).info
            cur_price = cur_info['regularMarketPrice']
            cur_cur = cur_info['currency']
            if df_depot.loc[mask_bank]['value_eur'].values[0] >= INVEST_VALUE:
                VALUE = INVEST_VALUE
            elif df_depot.loc[mask_bank]['value_eur'].values[0] >= MIN_INVEST_VALUE:
                VALUE = df_depot.loc[mask_bank]['value_eur'].values[0]
            cur_exr = f.update_exr(cur_exr, cur_cur)
            amount = VALUE // (cur_price / cur_exr[cur_cur])
            df_temp = pd.DataFrame({"type":"buy", 'isin':df_buy_opt.at[row.Index, 'isin'], "symbol":df_buy_opt.at[row.Index, 'symbol'],
                                     'symbol_finanzen':df_buy_opt.at[row.Index, "symbol_finanzen"], 'name': df_buy_opt.at[row.Index, 'name'],'buy_date':cur_time, 
                                     'price_buy':cur_price, 'cur':cur_cur, 'exr_hist':cur_exr[cur_cur],
                                      'price_buy_eur':cur_price / cur_exr[cur_cur], 'amount':amount, 
                                      'cur_date':cur_time, 'price_cur':cur_price, 'cur2':cur_cur, 
                                    'exr_cur':cur_exr[cur_cur], "price_cur_eur":cur_price/cur_exr[cur_cur],
                                    'value_org':cur_price * amount, 'value_eur':cur_price / cur_exr[cur_cur] * amount, 
                                    'stop_loss_eur':cur_price / cur_exr[cur_cur] * STOP_LOSS_PC, 
                                    'rendite_org':0.0, 'rendite_eur':0.0, 
                                    'lev_score': df_buy_opt.at[row.Index, 'lev_score']}, index=[0]) 
            df_transact = pd.concat([df_transact, df_temp]).reset_index(drop=True)
            df_depot = pd.concat([df_depot, df_temp.drop(columns=['type'])]).reset_index(drop=True)
            # reduce bank account value by purchase volume
            df_depot.at[0, 'value_eur'] = df_depot.at[0, "value_eur"] - cur_price / cur_exr[cur_cur] * amount    
        except Exception as err:
            print("2", row.symbol, err)
# delete stocks from depot
df_depot = df_depot.loc[~df_depot['isin'].isin(isin_sold)].reset_index(drop=True)

# 3. set new stop loss
df_depot['stop_loss_eur'] = np.where(df_depot['price_cur_eur'] * STOP_LOSS_PC > df_depot['stop_loss_eur'], df_depot['price_cur_eur'] * STOP_LOSS_PC, df_depot['stop_loss_eur']) 

# 4. add depot to depot historic
df_depot_hist = pd.concat([df_depot_hist, df_depot], axis=0).reset_index(drop=True)

# 5. save Files
df_depot_hist.to_excel(PATH + FILE_DEPOT_HIST, index=False)
df_depot.to_excel(PATH + FILE_DEPOT, index=False)
df_transact.to_excel(PATH + FILE_TRANSACTIONS, index=False)