import pandas as pd
from datetime import datetime, date, timedelta
import pathlib
import jqdatasdk as jqdata
from conbond.core import previous_trade_date


def auth(username, password):
    jqdata.auth(username, password)


def fetch(today=date.today(), cache_dir=None, username=None, password=None):
    txn_day = previous_trade_date(today)
    df_basic_info = None
    df_latest_bond_price = None
    df_latest_stock_price = None
    df_convert_price_adjust = None
    cache_path = None

    if cache_dir:
        cache_path = pathlib.Path(cache_dir).joinpath(
            'jqdata', '%s' % txn_day.strftime('%Y-%m-%d'))

    if cache_path and cache_path.exists():
        print('Using cached file: %s' % cache_path)
        df_basic_info = pd.read_excel(
            cache_path.parent.joinpath('conbond_basic_info.xlsx'))
        df_convert_price_adjust = pd.read_excel(
            cache_path.parent.joinpath('conbond_convert_price_adjust.xlsx'))
        df_latest_bond_price = pd.read_excel(
            cache_path.joinpath('conbond_daily_price.xlsx'))
        df_latest_stock_price = pd.read_excel(
            cache_path.joinpath('conbond_stock_daily_price.xlsx'))
    else:
        auth(username, password)
        txn_day, df_basic_info, df_convert_price_adjust, df_latest_bond_price, df_latest_stock_price = read_data(
            today)
        if cache_path:
            cache_path.mkdir(parents=True, exist_ok=True)
            df_basic_info.to_excel(
                cache_path.parent.joinpath('conbond_basic_info.xlsx'))
            df_convert_price_adjust.to_excel(
                cache_path.parent.joinpath(
                    'conbond_convert_price_adjust.xlsx'))
            df_latest_bond_price.to_excel(
                cache_path.joinpath('conbond_daily_price.xlsx'))
            df_latest_stock_price.to_excel(
                cache_path.joinpath('conbond_stock_daily_price.xlsx'))

    return process(txn_day, df_basic_info, df_convert_price_adjust,
                   df_latest_bond_price, df_latest_stock_price)


def read_data(today):
    txn_day = jqdata.get_trade_days(end_date=(today - timedelta(days=1)),
                                    count=1)[0]
    df_basic_info = jqdata.bond.run_query(
        jqdata.query(jqdata.bond.CONBOND_BASIC_INFO))
    # For some reason some company_code is nan
    df_basic_info = df_basic_info[pd.notnull(df_basic_info['company_code'])]
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
    assert (len(df_convert_price_adjust) < 5000)
    return txn_day, df_basic_info, df_convert_price_adjust, df_latest_bond_price, df_latest_stock_price


def process(txn_day, df_basic_info, df_convert_price_adjust,
            df_latest_bond_price, df_latest_stock_price):
    # Convert code, company_code to string for joining
    df_basic_info['code'] = df_basic_info.code.astype(str)
    df_basic_info['company_code'] = df_basic_info.company_code.astype(str)
    df_convert_price_adjust['code'] = df_convert_price_adjust.code.astype(str)
    df_latest_bond_price['code'] = df_latest_bond_price.code.astype(str)
    df_latest_stock_price['code'] = df_latest_stock_price.code.astype(str)

    # Keep only bonds that are listed and also can be traded
    # Some bonds are still listed, but is not traded (e.g. 2021-08-26, 123029)
    df_latest_bond_price = df_latest_bond_price[df_latest_bond_price.close > 0]
    df_latest_bond_price = df_latest_bond_price[[
        'code', 'exchange_code', 'close'
    ]].rename(columns={'close': 'bond_price'})

    df_basic_info = df_basic_info[[
        'code',
        'short_name',
        'company_code',
    ]]

    # Schema: code, short_name, company_code, bond_price, exchange_code
    df = df_latest_bond_price.set_index('code').join(
        df_basic_info.set_index('code')).reset_index()

    # Filter price adjust so that the convert price is the correct one at that day
    # Using the latest one is wrong as it can be sometime newer than txn_day
    df_convert_price_adjust['adjust_date'] = pd.to_datetime(
        df_convert_price_adjust['adjust_date'])
    df_convert_price_adjust = df_convert_price_adjust[
        df_convert_price_adjust['adjust_date'].dt.date <= txn_day]
    df_convert_price_adjust = df_convert_price_adjust[[
        'code', 'new_convert_price'
    ]].groupby('code').min()
    df_convert_price_adjust = df_convert_price_adjust.rename(
        columns={'new_convert_price': 'convert_price'})

    # Join with convert_price_adjust to get latest convert price
    # Schema: code, short_name, company_code, bond_price, convert_price, exchange_code
    df = df.set_index('code').join(df_convert_price_adjust)

    df_latest_stock_price = df_latest_stock_price[[
        'code', 'close'
    ]].rename(columns={'close': 'stock_price'})
    # Join with latest_stock_price to get latest stock price
    # Schema: code, short_name, company_code, bond_price, convert_price, stock_price, exchange_code
    df = df.reset_index().set_index('company_code').join(
        df_latest_stock_price.set_index('code'))

    # Calculate convert_premium_rate
    # Schema: code, short_name, company_code, bond_price, convert_price, stock_price, convert_premium_rate, exchange_code
    df['convert_premium_rate'] = df.bond_price / (100 / df.convert_price *
                                                  df.stock_price) - 1

    df['code'] = df[['code', 'exchange_code']].agg('.'.join, axis=1)
    df = df.drop(columns=['exchange_code'])
    return txn_day, df
