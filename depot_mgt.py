import openpyxl
import pandas as pd
import numpy as np
import yfinance as yf
import warnings
import os
import time
import functions as f
import requests

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
TRADING_FEE = 3
GOOD_WEEKLY_PERFORMANCE = 0.01

# instantiate message for telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
print(all([TELEGRAM_TOKEN]))
print(all([CHAT_ID]))
if not all([TELEGRAM_TOKEN, CHAT_ID]):
    from credentials import TELEGRAM_TOKEN, CHAT_ID
message = ""

# 0. load relevant files
df_result = pd.read_excel(PATH + FILE_RESULT_DAY)
if not os.path.exists(PATH + FILE_DEPOT):
    # initialize bank account
    df_depot = pd.DataFrame({'isin':'bank', "symbol":'bank', 'symbol_finanzen':'bank','name':'account', 'buy_date':'2025-03-16',
                             'price_buy':1.00, 'cur':'EUR', 'exr_hist':1, 'price_buy_eur':1, 'amount':1,
                               'cur_date':'2025-03-17', 'price_cur':1.00, 'cur2':'EUR', 'exr_cur':1, 
                               "price_cur_eur":1, 'value_org':0, 'value_eur':10000, 'stop_loss_eur':0.00,
                                'rendite_org':0.00001, 'rendite_eur':0.00001, 'lev_score': 100.00, 'tax_cum':0.00}, index=[0])
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

# 2.2 sell based on fixed values
mask_1 = df_depot['lev_score'] <= MIN_L_SCORE
mask_2 = df_depot['price_cur_eur'] <= df_depot['stop_loss_eur']
df_sales = df_depot.loc[mask_1 | mask_2].copy().reset_index(drop=True)
for row in df_sales.itertuples():
    # do not sales a stock you would directly rebuy:
    if row.isin in df_pur_opt['isin'].unique():
        df_pur_opt.at[df_pur_opt.loc[df_pur_opt['isin'] == row.isin].index[0], 'in_dpt'] = 0
    if df_pur_opt.loc[df_pur_opt['in_dpt'] == 0].head(1)['isin'].values[0] == row.isin:
        df_pur_opt.at[df_pur_opt.loc[df_pur_opt['isin'] == row.isin].index[0], 'in_dpt'] = 1
        print("no rebuy sales!")
        continue
    # else sell the stock and add money to bank (consider taxes at rendite_eur column of bank)
    df_depot, taxes_cum = f.update_bank_and_taxes(df_depot, row, TAX, TRADING_FEE) # TODO check if df_depot get's changed
    df_temp = f.create_sales_info(df_sales, row, taxes_cum, TRADING_FEE)
    df_transact = pd.concat([df_transact, df_temp])
    # add to telegram message
    message += f.add_to_message("sell", df_temp)
    # delete stocks from depot
    df_depot = df_depot.loc[df_depot['isin'] != row.isin].reset_index(drop=True)

# 2.3 buy with bank money
# buy the best stocks from bank
mask_not_in_depot = df_pur_opt['in_dpt'] == 0
for row in df_pur_opt.loc[mask_not_in_depot].itertuples():
    # buy all for invest value and last one for min value
    if df_depot.at[0, 'value_eur'] >= INVEST_VALUE:
        VALUE = INVEST_VALUE - TRADING_FEE
    elif df_depot.at[0, 'value_eur'] >= MIN_INVEST_VALUE:
        VALUE = df_depot.at[0, 'value_eur'] - TRADING_FEE
    else:
        break
    try:
        # calculate purchase and all fields
        df_temp = f.buy_stock(row, VALUE, cur_time, cur_exr, df_depot.at[0, 'tax_cum'], STOP_LOSS_PC, TRADING_FEE)
        # add it to transactions
        df_transact = pd.concat([df_transact, df_temp]).reset_index(drop=True)
        # update depot
        df_depot = pd.concat([df_depot, df_temp.drop(columns=['type', 'fee'])]).reset_index(drop=True)
        df_depot.at[0, 'value_eur'] -= (df_temp['value_eur'].values[0] + TRADING_FEE)
        # set in depot variable to 1
        df_pur_opt.at[row.Index, 'in_dpt'] = 1
        # add to telegram message
        message += f.add_to_message("buy", df_temp)
    except Exception as err:
        print("1", row.symbol, err)
    
# 2.4. shift stocks to better options
# prepare depot dataframe
mask_bank = df_depot['isin'] == 'bank'
df_sales = df_depot.loc[~mask_bank].sort_values(['lev_score', 'rendite_eur']).copy().reset_index(drop=True)
for i, row in enumerate(df_sales.itertuples()):
    print(i)
    # get possible stocks
    mask_cur = df_result['isin'].isin(df_depot['isin'].unique())
    mask_shift_stocks = df_result['lev_score'] >= df_depot['lev_score'].min() + THRES_LEV_BETTER
    df_buy_opt = df_result.loc[mask_shift_stocks & ~mask_cur].sort_values(['lev_score', 'market_cap'], ascending=[False, False]).reset_index(drop=True)
    # stop if no more options available
    if df_buy_opt.empty:
        print("switch: no - no more options available")
        break
    # buy only if other stock is at least x-scores better and current store has no good performance
    holding_weeks = (row.cur_date - row.buy_date).days / 7
    # if bought today skip
    if holding_weeks == 0:
        print(f'switch: no - {row.symbol} bought today')
        continue
    weekly_performance = row.rendite_eur / holding_weeks
    if row.lev_score + THRES_LEV_BETTER < df_buy_opt.at[0, 'lev_score'] \
    and weekly_performance < GOOD_WEEKLY_PERFORMANCE: 
        try: 
            # check if bank account after sales is high enough for switch
            taxes = max(0, (row.price_cur_eur - row.price_buy_eur) * row.amount + df_depot.at[0, 'tax_cum'])
            if df_depot.at[0, 'value_eur'] + row.value_eur - TRADING_FEE - taxes < MIN_INVEST_VALUE:
                print(f"switch: no - {row.symbol} value too low")
                continue
            # sell the stock and add money to bank (consider taxes at rendite_eur column of bank)
            df_depot, taxes_cum = f.update_bank_and_taxes(df_depot, row, TAX, TRADING_FEE) # TODO check if df_depot get's changed
            df_temp = f.create_sales_info(df_sales, row, taxes_cum, TRADING_FEE)
            df_transact = pd.concat([df_transact, df_temp])
            # delete stocks from depot
            df_depot = df_depot.loc[df_depot['isin'] != row.isin].reset_index(drop=True)
            # add to telegram message
            message += f.add_to_message("sell opt", df_temp)
            # buy new stock
            if df_depot.at[0, 'value_eur'] >= INVEST_VALUE:
                VALUE = INVEST_VALUE - TRADING_FEE
            elif df_depot.at[0, 'value_eur'] >= MIN_INVEST_VALUE:
                VALUE = df_depot.at[0, 'value_eur'] - TRADING_FEE
            else:
                break
            # TODO: no row but different dataframe
            for row_buy_opt in df_buy_opt.iloc[:1].itertuples():
                df_temp = f.buy_stock(row_buy_opt, VALUE, cur_time, cur_exr, df_depot.at[0, 'tax_cum'], STOP_LOSS_PC, TRADING_FEE)
            # add it to transactions
            df_transact = pd.concat([df_transact, df_temp]).reset_index(drop=True)
            # update depot
            df_depot = pd.concat([df_depot, df_temp.drop(columns=['type', 'fee'])]).reset_index(drop=True)
            df_depot.at[0, 'value_eur'] -= (df_temp['value_eur'].values[0] + TRADING_FEE)
            # add to telegram message
            message += f.add_to_message("buy opt", df_temp)
        except Exception as err:
            print("error handler", row.symbol, err)

# 3. set new stop loss
df_depot['stop_loss_eur'] = np.where(df_depot['price_cur_eur'] * STOP_LOSS_PC > df_depot['stop_loss_eur'], 
                                     df_depot['price_cur_eur'] * STOP_LOSS_PC, df_depot['stop_loss_eur']) 

# 4. add depot to depot historic
df_depot_hist = pd.concat([df_depot_hist, df_depot], axis=0).reset_index(drop=True)

# 5. save Files
df_depot_hist.to_excel(PATH + FILE_DEPOT_HIST, index=False)
df_depot.to_excel(PATH + FILE_DEPOT, index=False)
df_transact.to_excel(PATH + FILE_TRANSACTIONS, index=False)

# 6. send message to telegram
url_send = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
if message == "":
    message = "no trades made"

payload = {
    'chat_id': CHAT_ID,
    'text': message
}
response = requests.post(url_send, data=payload)
if response.status_code == 200:
    print('Message sent successfully!')
else:
    print('Message not sent!')