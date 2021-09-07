import pandas as pd
from datetime import datetime, date, timedelta
import pathlib
import rqdatac


def auth(username, password):
    rqdatac.init(username, password)


def fetch(
        today=date.today(), cache_dir=None, username=None, password=None):
    txn_day = previous_trade_date(today)
    df_basic_info = None
    df_latest_bond_price = None
    df_latest_stock_price = None
    df_convert_price_adjust = None
    cache_path = None

    if cache_dir:
        cache_path = pathlib.Path(cache_dir).joinpath(
            'rqdata', 'bond', '%s.xlsx' % txn_day.strftime('%Y-%m-%d'))
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(cache_dir).joinpath('rqdata',
                                         'stock').mkdir(parents=True,
                                                        exist_ok=True)

    if cache_path.exists():
        rpath = cache_path.parent.parent
        df_basic_info = pd.read_excel(rpath.joinpath('basic_info.xlsx'))
        df_convert_price_adjust = pd.read_excel(
            rpath.joinpath('convert_price_adjust.xlsx'))
        df_latest_bond_price = pd.read_excel(cache_path)
        df_latest_stock_price = pd.read_excel(
            rpath.joinpath('stock', '%s.xlsx' % txn_day.strftime('%Y-%m-%d')))
    else:
        auth(username, password)
        txn_day, df_basic_info, df_convert_price_adjust, df_latest_bond_price, df_latest_stock_price = read_data(
            today)
        if cache_path:
            rpath = cache_path.parent.parent
            df_basic_info.to_excel(rpath.joinpath('basic_info.xlsx'))
            df_convert_price_adjust.to_excel(
                rpath.joinpath('convert_price_adjust.xlsx'))
            df_latest_bond_price.to_excel(cache_path)
            df_latest_stock_price.to_excel(
                rpath.joinpath('stock',
                               '%s.xlsx' % txn_day.strftime('%Y-%m-%d')))

    return process(txn_day, df_basic_info, df_convert_price_adjust,
                   df_latest_bond_price, df_latest_stock_price)


def read_data(today):
    txn_day = rqdatac.get_previous_trading_date(today)
    df_basic_info = rqdatac.convertible.all_instruments(txn_day).reset_index()
    df_latest_bond_price = rqdatac.get_price(
        df_basic_info.order_book_id.tolist(),
        start_date=txn_day,
        end_date=txn_day,
        frequency='1d').reset_index()
    df_latest_stock_price = rqdatac.get_price(
        df_basic_info.stock_code.tolist(),
        start_date=txn_day,
        end_date=txn_day,
        frequency='1d').reset_index()
    df_convert_price_adjust = rqdatac.convertible.get_conversion_price(
        df_basic_info.order_book_id.tolist(), end_date=txn_day).reset_index()
    return txn_day, df_basic_info, df_convert_price_adjust, df_latest_bond_price, df_latest_stock_price


def process(txn_day, df_basic_info, df_convert_price_adjust,
            df_latest_bond_price, df_latest_stock_price):
    # Data cleaning
    # Filter non-conbond, e.g. exchange bond
    df_basic_info = df_basic_info[df_basic_info.bond_type == 'cb']
    df_basic_info = df_basic_info[[
        'order_book_id',
        'symbol',
        'stock_code',
    ]]
    df_latest_bond_price = df_latest_bond_price[[
        'order_book_id', 'close'
    ]].rename(columns={'close': 'bond_price'})
    # Join basic_info with latest_bond_price to get close price from last transaction day
    # Schema: code, short_name, company_code, bond_price
    df = df_basic_info.set_index('order_book_id').join(
        df_latest_bond_price.set_index('order_book_id')).reset_index()

    df_convert_price_adjust = df_convert_price_adjust[[
        'order_book_id', 'conversion_price'
    ]].groupby('order_book_id').min()
    df_convert_price_adjust = df_convert_price_adjust.rename(
        columns={'conversion_price': 'convert_price'})

    # Schema: order_book_id, symbol, stock_code, bond_price, convert_price
    df = df.set_index('order_book_id').join(df_convert_price_adjust)

    df_latest_stock_price = df_latest_stock_price[[
        'order_book_id', 'close'
    ]].rename(columns={'close': 'stock_price'})
    # Join with latest_stock_price to get latest stock price
    # Schema: order_book_id, short_name, company_code, bond_price, convert_price, stock_price
    df = df.reset_index().set_index('stock_code').join(
        df_latest_stock_price.set_index('order_book_id'))

    # Calculate convert_premium_rate
    # Schema: order_book_id, symbol, stock_code, bond_price, convert_price, stock_price, convert_premium_rate
    df['convert_premium_rate'] = df.bond_price / (100 / df.convert_price *
                                                  df.stock_price) - 1
    df = df.rename(columns={'order_book_id': 'code', 'symbol': 'short_name'})
    return txn_day, df
