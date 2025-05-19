import openpyxl
import pandas as pd
import numpy as np
import yfinance as yf
import warnings
import os
import time
import functions as f
import requests
import logging
import datetime as dt
import sys


def main():
    warnings.simplefilter('ignore', 'FutureWarning')
    logging.basicConfig(filename='./data/data_pipeling.log', level=logging.INFO)

    PATH = "./data/"
    FILE_RESULT_DAY  = "result.xlsx"
    FILE_DEPOT = "depot.xlsx"
    FILE_DEPOT_HIST = "depot_hist.xlsx"
    FILE_TRANSACTIONS = "transactions.xlsx"

    L_SCORE_BUY = 9
    
    MIN_L_SCORE = 7
    STOP_LOSS_PC = 0.9
    THRES_LEV_BETTER = 3
    GOOD_WEEKLY_PERFORMANCE = 0.01
    LEV_LOSS_PC = 0.2
    
    INVEST_VALUE = 1500
    MIN_INVEST_VALUE = 1000
    TAX = 0.25
    TRADING_FEE = 3

    # instantiate message for telegram
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    CHAT_ID = os.getenv("CHAT_ID")
    if not all([TELEGRAM_TOKEN, CHAT_ID]):
        from credentials import TELEGRAM_TOKEN, CHAT_ID
    message = ""

    # 0. load relevant files
    df_result = pd.read_excel(PATH + FILE_RESULT_DAY)
    if not os.path.exists(PATH + FILE_DEPOT):
        # initialize bank account
        df_depot = pd.DataFrame({'isin':'bank', "symbol":'bank', 'symbol_finanzen':'bank','name':'account', 'buy_date':'2025-03-16',
                                'price_buy':1.00, 'cur':'EUR', 'exr_hist':1, 'price_buy_eur':1,'amount':1, 'lev_buy':100,
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
            f.update_depot(df_depot, row, cur_time, cur_exr)
        except Exception as err:
            print("0", row.symbol, err)
            logging.info(f"Exception depot manager update data: {row.Index} {row.symbol}")
            logging.error(err, stack_info=True, exc_info=True)
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
    mask_3 = df_depot['lev_score'] <= df_depot['lev_buy'] * (1 - LEV_LOSS_PC)
    df_sales = df_depot.loc[mask_1 | mask_2 | mask_3].copy().reset_index(drop=True)
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
        message += f.add_to_message("sell rules", df_temp)
        # delete stocks from depot
        df_depot = df_depot.loc[df_depot['isin'] != row.isin].reset_index(drop=True)

    # 2.3 buy with bank money
    # buy the best stocks from bank
    mask_not_in_depot = df_pur_opt['in_dpt'] == 0
    for row in df_pur_opt.loc[mask_not_in_depot].itertuples():
        # buy all for invest value and last one for min value
        value = f.define_invest_value(df_depot.at[0, 'value_eur'], INVEST_VALUE, MIN_INVEST_VALUE, TRADING_FEE)
        if value is None:
            break 
        try:
            # calculate purchase and all fields
            df_temp = f.buy_stock(row, value, cur_time, cur_exr, df_depot.at[0, 'tax_cum'], STOP_LOSS_PC, TRADING_FEE)
            # add it to transactions
            df_transact = pd.concat([df_transact, df_temp]).reset_index(drop=True)
            # update depot
            df_depot = pd.concat([df_depot, df_temp.drop(columns=['type', 'fee'])]).reset_index(drop=True)
            df_depot.at[0, 'value_eur'] -= (df_temp['value_eur'].values[0] + TRADING_FEE)
            # set in depot variable to 1
            df_pur_opt.at[row.Index, 'in_dpt'] = 1
            # add to telegram message
            message += f.add_to_message("buy rules", df_temp)
        except Exception as err:
            print("1", row.symbol, err)
            logging.info(f"{dt.datetime.now().strftime('%d.%m.%Y %H:%M:%S')} Exception depot manager purchase normal: {row.Index} {row.symbol}")
            logging.error(err, stack_info=True, exc_info=True)
        
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
                value = f.define_invest_value(df_depot.at[0, 'value_eur'], INVEST_VALUE, MIN_INVEST_VALUE, TRADING_FEE)
                if value is None:
                   break 
                # TODO: no row but different dataframe
                for row_buy_opt in df_buy_opt.iloc[:1].itertuples():
                    df_temp = f.buy_stock(row_buy_opt, value, cur_time, cur_exr, df_depot.at[0, 'tax_cum'], STOP_LOSS_PC, TRADING_FEE)
                # add it to transactions
                df_transact = pd.concat([df_transact, df_temp]).reset_index(drop=True)
                # update depot
                df_depot = pd.concat([df_depot, df_temp.drop(columns=['type', 'fee'])]).reset_index(drop=True)
                df_depot.at[0, 'value_eur'] -= (df_temp['value_eur'].values[0] + TRADING_FEE)
                # add to telegram message
                message += f.add_to_message("buy opt", df_temp)
            except Exception as err:
                print("error handler", row.symbol, err)
                logging.info(f"{dt.datetime.now().strftime('%d.%m.%Y %H:%M:%S')} Exception depot manager stock shift: {row.Index} {row.symbol}")
                logging.error(err, stack_info=True, exc_info=True)

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
    if message == "":
        message = "no trades"
    message += f"\nreturn: {np.round((df_depot['value_eur'].sum() / 10000 -1) * 100, 2)}%"
    status = f.send_telegram_msg(message, TELEGRAM_TOKEN, CHAT_ID)
    print(status)

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logging.info(f"{dt.datetime.now().strftime('%d.%m.%Y %H:%M:%S')} Exception depot manager main")
        logging.error(err, stack_info=True, exc_info=True)
        sys.exit(1)