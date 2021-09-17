# -*- coding: utf-8 -*-
from datetime import date
import rqdatac
import pandas as pd
import pathlib


def read_or_none(cache_path, f):
    p = cache_path.joinpath(f)
    if p.exists():
        return pd.read_excel(p)
    return None


def fetch(txn_day, cache_dir=None, logger=None):
    df_all_instruments = None
    df_conversion_price = None
    df_latest_bond_price = None
    df_latest_stock_price = None
    df_call_info = None
    df_indicators = None
    df_suspended = None
    cache_path = None
    cached = []
    fetched = []

    if cache_dir:
        cache_path = pathlib.Path(cache_dir).joinpath(
            'rqdata', txn_day.strftime('%Y-%m-%d'))
        cache_path.mkdir(parents=True, exist_ok=True)
        df_all_instruments = read_or_none(cache_path, 'all_instruments.xlsx')
        df_conversion_price = read_or_none(cache_path, 'conversion_price.xlsx')
        df_latest_bond_price = read_or_none(cache_path, 'bond_price.xlsx')
        df_latest_stock_price = read_or_none(cache_path, 'stock_price.xlsx')
        df_call_info = read_or_none(cache_path, 'call_info.xlsx')
        df_indicators = read_or_none(cache_path, 'indicators.xlsx')
        df_suspended = read_or_none(cache_path, 'suspended.xlsx')

    if df_all_instruments is None:
        fetched.append('all_instruments')
        df_all_instruments = rqdatac.convertible.all_instruments(
            txn_day).reset_index()
        if cache_path:
            df_all_instruments.to_excel(
                cache_path.joinpath('all_instruments.xlsx'))
    else:
        cached.append('all_instruments')

    if df_latest_bond_price is None:
        fetched.append('bond_price')
        df_latest_bond_price = rqdatac.get_price(
            df_all_instruments.order_book_id.tolist(),
            start_date=txn_day,
            end_date=txn_day,
            frequency='1d').reset_index()
        if cache_path:
            df_latest_bond_price.to_excel(
                cache_path.joinpath('bond_price.xlsx'))
    else:
        cached.append('bond_price')

    if df_latest_stock_price is None:
        fetched.append('stock_price')
        df_latest_stock_price = rqdatac.get_price(
            df_all_instruments.stock_code.tolist(),
            start_date=txn_day,
            end_date=txn_day,
            frequency='1d').reset_index()
        if cache_path:
            df_latest_stock_price.to_excel(
                cache_path.joinpath('stock_price.xlsx'))
    else:
        cached.append('stock_price')

    if df_conversion_price is None:
        fetched.append('conversion_price')
        df_conversion_price = rqdatac.convertible.get_conversion_price(
            df_all_instruments.order_book_id.tolist(),
            end_date=txn_day).reset_index()
        if cache_path:
            df_conversion_price.to_excel(
                cache_path.joinpath('conversion_price.xlsx'))
    else:
        cached.append('conversion_price')

    if df_call_info is None:
        fetched.append('call_info')
        df_call_info = rqdatac.convertible.get_call_info(
            df_all_instruments.order_book_id.tolist(), end_date=txn_day)
        if df_call_info is not None:
            df_call_info = df_call_info.reset_index()
            if cache_path:
                df_call_info.to_excel(cache_path.joinpath('call_info.xlsx'))
    else:
        cached.append('call_info')

    if df_indicators is None:
        fetched.append('indicators')
        df_indicators = rqdatac.convertible.get_indicators(
            df_all_instruments.order_book_id.tolist(),
            start_date=txn_day,
            end_date=txn_day).reset_index()
        if cache_path:
            df_indicators.to_excel(cache_path.joinpath('indicators.xlsx'))
    else:
        cached.append('indicators')

    if df_suspended is None:
        fetched.append('suspended')
        df_suspended = rqdatac.convertible.is_suspended(
            df_all_instruments.order_book_id.tolist(),
            start_date=txn_day,
            end_date=txn_day)
        if cache_path:
            df_suspended.to_excel(cache_path.joinpath('suspended.xlsx'))
    else:
        cached.append('suspended')

    if logger:
        logger.info('%s: fetched: %s' % (txn_day.strftime('%Y-%m-%d'), fetched))
    return txn_day, df_all_instruments, df_conversion_price, df_latest_bond_price, df_latest_stock_price, df_call_info, df_indicators, df_suspended


def process(txn_day, df_all_instruments, df_conversion_price,
            df_latest_bond_price, df_latest_stock_price, df_call_info,
            df_indicators, df_suspended):
    # Data cleaning
    # Filter non-conbond, e.g. exchange bond
    df_all_instruments = df_all_instruments[df_all_instruments.bond_type ==
                                            'cb']
    # Filter bonds that stopped trading by txn_day
    df_all_instruments = df_all_instruments.assign(stopped_trading=lambda row: row.stop_trading_date.dt.date <= txn_day)
    df_all_instruments = df_all_instruments[df_all_instruments.stopped_trading
                                            == False]

    df_all_instruments = df_all_instruments[[
        'order_book_id',
        'symbol',
        'stock_code',
    ]]

    df_latest_stock_price = df_latest_stock_price[[
        'order_book_id', 'close'
    ]].rename(columns={
        'close': 'stock_price'
    }).set_index('order_book_id')
    # stock_price
    df = df_all_instruments.set_index('stock_code').join(
        df_latest_stock_price).reset_index().set_index('order_book_id')

    df_latest_bond_price = df_latest_bond_price[[
        'order_book_id', 'close'
    ]].rename(columns={
        'close': 'bond_price'
    }).set_index('order_book_id')
    # bond_price
    df = df.join(df_latest_bond_price)
    if df_call_info is not None and 'info_date' in df_call_info.columns:
        # info_date
        df_call_info = df_call_info[pd.notnull(df_call_info.info_date)]
        if not df_call_info.empty:
            df = df.join(df_call_info[['order_book_id', 'info_date'
                                       ]].set_index('order_book_id'))
            # TODO: Check why, it happens on 08-20
            if df.info_date.dt.date.dtype == date:
                df['force_redeem'] = df.info_date.dt.date < txn_day
                df = df[df.force_redeem == False]

    df_conversion_price = df_conversion_price[[
        'order_book_id', 'conversion_price'
    ]].groupby('order_book_id').min()
    # conversion_price
    df = df.join(df_conversion_price)

    df['convert_premium_rate'] = df.bond_price / (100 / df.conversion_price *
                                                  df.stock_price) - 1
    return df


def auth(username, password):
    rqdatac.init(username, password)
