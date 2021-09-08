# -*- coding: utf-8 -*-
from datetime import date
import rqdatac
import pandas as pd
import pathlib
from conbond.core import previous_trade_date


def read_data(today):
    txn_day = rqdatac.get_previous_trading_date(today)
    df_all_instruments = rqdatac.convertible.all_instruments(
        txn_day).reset_index()
    df_latest_bond_price = rqdatac.get_price(
        df_all_instruments.order_book_id.tolist(),
        start_date=txn_day,
        end_date=txn_day,
        frequency='1d').reset_index()
    df_latest_stock_price = rqdatac.get_price(
        df_all_instruments.stock_code.tolist(),
        start_date=txn_day,
        end_date=txn_day,
        frequency='1d').reset_index()
    df_conversion_price = rqdatac.convertible.get_conversion_price(
        df_all_instruments.order_book_id.tolist(),
        end_date=txn_day).reset_index()
    df_call_info = rqdatac.convertible.get_call_info(
        df_all_instruments.order_book_id.tolist(),
        end_date=txn_day)
    if df_call_info is not None:
        df_call_info = df_call_info.reset_index()
    else:
        df_call_info = pd.DataFrame()
    df_indicators = rqdatac.convertible.get_indicators(
        df_all_instruments.order_book_id.tolist(),
        start_date=txn_day,
        end_date=txn_day).reset_index()
    return txn_day, df_all_instruments, df_conversion_price, df_latest_bond_price, df_latest_stock_price, df_call_info, df_indicators


def process(txn_day, df_all_instruments, df_conversion_price, df_latest_bond_price,
            df_latest_stock_price, df_call_info, df_indicators):
    # Data cleaning
    # Filter non-conbond, e.g. exchange bond
    df_all_instruments = df_all_instruments[df_all_instruments.bond_type ==
                                            'cb']
    df_all_instruments = df_all_instruments[[
        'order_book_id',
        'symbol',
        'stock_code',
    ]]
    df_latest_bond_price = df_latest_bond_price[[
        'order_book_id', 'close'
    ]].rename(columns={'close': 'bond_price'})
    df = df_all_instruments.set_index('order_book_id').join(
        df_latest_bond_price.set_index('order_book_id'))
    df = df.join(df_call_info[['order_book_id', 'info_date']].set_index('order_book_id'))
    df['force_redeem'] = df.info_date.dt.date < txn_day
    df = df[df.force_redeem == False]

    df_conversion_price = df_conversion_price[[
        'order_book_id', 'conversion_price'
    ]].groupby('order_book_id').min()

    df = df.join(df_conversion_price)

    df_latest_stock_price = df_latest_stock_price[[
        'order_book_id', 'close'
    ]].rename(columns={'close': 'stock_price'})
    df = df.reset_index().set_index('stock_code').join(
        df_latest_stock_price.set_index('order_book_id'))

    df['convert_premium_rate'] = df.bond_price / (100 / df.conversion_price *
                                                  df.stock_price) - 1
    return df


def fetch(today=date.today(), cache_dir=None, skip_process=False):
    txn_day = previous_trade_date(today)
    df_all_instruments = None
    df_conversion_price = None
    df_latest_bond_price = None
    df_latest_stock_price = None
    df_call_info = None
    df_indicators = None
    cache_path = None

    if cache_dir:
        cache_path = pathlib.Path(cache_dir).joinpath(
            'rqdata', txn_day.strftime('%Y-%m-%d'))

    if cache_path and cache_path.exists():
        print('Using cached file: %s' % cache_path)
        df_all_instruments = pd.read_excel(
            cache_path.joinpath('all_instruments.xlsx'))
        df_conversion_price = pd.read_excel(
            cache_path.joinpath('conversion_price.xlsx'))
        df_latest_bond_price = pd.read_excel(
            cache_path.joinpath('bond_price.xlsx'))
        df_latest_stock_price = pd.read_excel(
            cache_path.joinpath('stock_price.xlsx'))
        df_call_info = pd.read_excel(cache_path.joinpath('call_info.xlsx'))
        df_indicators = pd.read_excel(cache_path.joinpath('indicators.xlsx'))
    else:
        txn_day, df_all_instruments, df_conversion_price, df_latest_bond_price, df_latest_stock_price, df_call_info, df_indicators = read_data(
            today)
        print('Using data from: %s' % txn_day)
        if cache_path:
            cache_path.mkdir(parents=True, exist_ok=True)
            df_all_instruments.to_excel(
                cache_path.joinpath('all_instruments.xlsx'))
            df_conversion_price.to_excel(
                cache_path.joinpath('conversion_price.xlsx'))
            df_latest_bond_price.to_excel(
                cache_path.joinpath('bond_price.xlsx'))
            df_latest_stock_price.to_excel(
                cache_path.joinpath('stock_price.xlsx'))
            df_call_info.to_excel(cache_path.joinpath('call_info.xlsx'))
            df_indicators.to_excel(cache_path.joinpath('indicators.xlsx'))

    if not skip_process:
        return txn_day, process(txn_day, df_all_instruments, df_conversion_price,
                            df_latest_bond_price, df_latest_stock_price,
                            df_call_info, df_indicators)


def auth(username, password):
    rqdatac.init(username, password)
