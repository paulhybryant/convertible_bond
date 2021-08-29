import numpy as np
import os
import pandas as pd
from datetime import date, timedelta
from collections.abc import Callable


# To use this locally, need to call auth() first
def fetch_jqdata(jqdata, today):
    txn_day = jqdata.get_trade_days(end_date=(today - timedelta(days=1)),
                                    count=1)[0]
    df_basic_info = jqdata.bond.run_query(
        jqdata.query(jqdata.bond.CONBOND_BASIC_INFO))
    # Filter non-conbond, e.g. exchange bond
    df_basic_info = df_basic_info[df_basic_info.bond_type_id == 703013]
    # Keep active bonds only
    df_basic_info = df_basic_info[df_basic_info.list_status_id == 301001]
    df_latest_bond_price = jqdata.bond.run_query(
        jqdata.query(jqdata.bond.CONBOND_DAILY_PRICE).filter(
            jqdata.bond.CONBOND_DAILY_PRICE.date == txn_day))
    df_latest_stock_price = jqdata.get_price(
        df_basic_info.company_code.tolist(),
        start_date=txn_day,
        end_date=txn_day,
        frequency='daily',
        panel=False)
    df_convert_price_adjust = jqdata.bond.run_query(
        jqdata.query(jqdata.bond.CONBOND_CONVERT_PRICE_ADJUST))
    return txn_day, df_basic_info, df_latest_bond_price, df_latest_stock_price, df_convert_price_adjust


def massage_data(df_basic_info, df_latest_bond_price, df_latest_stock_price,
                 df_convert_price_adjust):
    # Data cleaning
    df_basic_info = df_basic_info[[
        'code', 'short_name', 'company_code', 'convert_price'
    ]]
    df_latest_bond_price = df_latest_bond_price[[
        'code', 'exchange_code', 'close'
    ]].rename(columns={'close': 'bond_price'})
    df_latest_stock_price = df_latest_stock_price[[
        'code', 'close'
    ]].rename(columns={'close': 'stock_price'})
    df_convert_price_adjust = df_convert_price_adjust[[
        'code', 'new_convert_price'
    ]].groupby('code').min()

    # Join basic_info with latest_bond_price to get close price from last transaction day
    # Schema: code, short_name, company_code, convert_price, bond_price
    df = df_basic_info.set_index('code').join(
        df_latest_bond_price.set_index('code')).reset_index()
    # Keep only bonds that are listed and also can be traded
    # Some bonds are still listed, but is not traded (e.g. 2021-08-26, 123029)
    df = df[df.bond_price > 0]

    # Join with convert_price_adjust to get latest convert price
    # code in convert_price_latest is str, while code in df is int64
    df['code'] = df.code.astype(str)
    # Schema: code, short_name, company_code, convert_price, bond_price, new_convert_price
    df = df.set_index('code').join(df_convert_price_adjust)

    # Join with latest_stock_price to get latest stock price
    # Schema: code, short_name, company_code, convert_price, bond_price, new_convert_price, stock_price
    df = df.reset_index().set_index('company_code').join(
        df_latest_stock_price.set_index('code'))

    # Calculate convert_premium_rate
    # Schema: code, short_name, company_code, convert_price, bond_price, new_convert_price, stock_price, convert_premium_rate
    df['convert_premium_rate'] = df.bond_price / (100 / df.new_convert_price *
                                                  df.stock_price) - 1
    return df


# config: Expect to have two keys: weight_bond_price and weight_convert_premium_rate
def double_low(df, config):
    dl_df = df
    if not 'weight_bond_price' in config:
        raise 'Bad config: weight_bond_price not found'
    if not 'weight_convert_premium_rate' in config:
        raise 'Bad config: weight_convert_premium_rate not found'
    if not 'top' in config:
        raise 'Bad config: top not found'
    weight_bond_price = config['weight_bond_price']
    weight_convert_premium_rate = config['weight_convert_premium_rate']
    top = config['top']
    dl_df[
        'double_low'] = df.bond_price * weight_bond_price + df.convert_premium_rate * 100 * weight_convert_premium_rate
    return dl_df.nsmallest(top, 'double_low')


def generate_orders(holdings, candidates):
    orders = {}
    orders['buy'] = candidates - holdings
    orders['sell'] = holdings - candidates
    orders['hold'] = holdings & candidates
    return orders


def execute_strategy(df, strategy, config):
    return strategy(df, config)


def fetch_cache(cache_dir):
    df_basic_info = pd.read_excel(os.path.join(cache_dir, 'basic_info.xlsx'))
    df_latest_bond_price = pd.read_excel(
        os.path.join(cache_dir, 'latest_bond_price.xlsx'))
    df_latest_stock_price = pd.read_excel(
        os.path.join(cache_dir, 'latest_stock_price.xlsx'))
    df_convert_price_adjust = pd.read_excel(
        os.path.join(cache_dir, 'convert_price_adjust.xlsx'))
    return df_latest_bond_price.date[
        0], df_basic_info, df_latest_bond_price, df_latest_stock_price, df_convert_price_adjust
