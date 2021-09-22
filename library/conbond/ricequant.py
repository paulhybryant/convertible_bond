# -*- coding: utf-8 -*-
from datetime import date
import rqdatac
import pandas as pd
import pathlib


def read_or_none(cache_path, f, logger, columns=[]):
    p = cache_path.joinpath(f)
    if p.exists():
        try:
            return pd.read_csv(p)
        except pd.errors.EmptyDataError:
            return pd.DataFrame(columns=columns)
    else:
        if logger:
            logger.info('Read %s with rqdatac' % f[:-4])
        return None


def fetch(txn_day, cache_dir=None, logger=None):
    df_all_instruments = None
    df_conversion_price = None
    #  df_conversion_info = None
    df_latest_bond_price = None
    df_latest_stock_price = None
    df_call_info = None
    df_put_info = None
    df_indicators = None
    df_suspended = None
    cache_path = None

    if logger:
        logger.info('Use data from: %s' % txn_day.strftime('%Y-%m-%d'))

    if cache_dir:
        cache_path = pathlib.Path(cache_dir).joinpath(
            'rqdata', txn_day.strftime('%Y-%m-%d'))
        cache_path.mkdir(parents=True, exist_ok=True)
        df_all_instruments = read_or_none(cache_path, 'all_instruments.csv',
                                          logger)
        df_conversion_price = read_or_none(cache_path, 'conversion_price.csv',
                                           logger)
        #  df_conversion_info = read_or_none(cache_path, 'conversion_info.csv',
                                          #  logger)
        # Remove after data is cached
        #  df_conversion_info = pd.DataFrame()
        df_latest_bond_price = read_or_none(cache_path, 'bond_price.csv',
                                            logger)
        df_latest_stock_price = read_or_none(cache_path, 'stock_price.csv',
                                             logger)
        df_call_info = read_or_none(cache_path, 'call_info.csv', logger, columns=['order_book_id', 'info_date'])
        #  df_put_info = read_or_none(cache_path, 'put_info.csv', logger)
        # Remove after data is cached
        df_put_info = pd.DataFrame()
        df_indicators = read_or_none(cache_path, 'indicators.csv', logger)
        df_suspended = read_or_none(cache_path, 'suspended.csv', logger)

    if df_all_instruments is None:
        df_all_instruments = rqdatac.convertible.all_instruments(
            txn_day).reset_index()
        if cache_path:
            df_all_instruments.to_csv(
                cache_path.joinpath('all_instruments.csv'), index=False)

    if df_latest_bond_price is None:
        df_latest_bond_price = rqdatac.get_price(
            df_all_instruments.order_book_id.tolist(),
            start_date=txn_day,
            end_date=txn_day,
            frequency='1d').reset_index()
        if cache_path:
            df_latest_bond_price.to_csv(cache_path.joinpath('bond_price.csv'),
                                        index=False)

    if df_latest_stock_price is None:
        df_latest_stock_price = rqdatac.get_price(
            df_all_instruments.stock_code.tolist(),
            start_date=txn_day,
            end_date=txn_day,
            frequency='1d').reset_index()
        if cache_path:
            df_latest_stock_price.to_csv(
                cache_path.joinpath('stock_price.csv'), index=False)

    if df_conversion_price is None:
        df_conversion_price = rqdatac.convertible.get_conversion_price(
            df_all_instruments.order_book_id.tolist(),
            end_date=txn_day).reset_index()
        if cache_path:
            df_conversion_price.to_csv(
                cache_path.joinpath('conversion_price.csv'), index=False)

    #  if df_conversion_info is None:
        #  df_conversion_info = rqdatac.convertible.get_conversion_info(
            #  df_all_instruments.order_book_id.tolist(),
            #  end_date=txn_day).reset_index()
        #  if cache_path:
            #  df_conversion_info.to_csv(
                #  cache_path.joinpath('conversion_info.csv'), index=False)

    if df_call_info is None:
        df_call_info = rqdatac.convertible.get_call_info(
            df_all_instruments.order_book_id.tolist(), end_date=txn_day)
        if df_call_info is None:
            df_call_info = pd.DataFrame(columns=['order_book_id', 'info_date'])
        else:
            df_call_info = df_call_info.reset_index()
        if cache_path:
            df_call_info.to_csv(cache_path.joinpath('call_info.csv'),
                                index=False)

    if df_put_info is None:
        df_put_info = rqdatac.convertible.get_put_info(
            df_all_instruments.order_book_id.tolist(), end_date=txn_day)
        if df_put_info is None:
            df_put_info = pd.DataFrame()
        else:
            df_put_info = df_put_info.reset_index()
        if cache_path:
            df_put_info.to_csv(cache_path.joinpath('put_info.csv'),
                               index=False)

    if df_indicators is None:
        df_indicators = rqdatac.convertible.get_indicators(
            df_all_instruments.order_book_id.tolist(),
            start_date=txn_day,
            end_date=txn_day).reset_index()
        if cache_path:
            df_indicators.to_csv(cache_path.joinpath('indicators.csv'),
                                 index=False)

    if df_suspended is None:
        df_suspended = rqdatac.convertible.is_suspended(
            df_all_instruments.order_book_id.tolist(),
            start_date=txn_day,
            end_date=txn_day)
        if cache_path:
            df_suspended.to_csv(cache_path.joinpath('suspended.csv'),
                                index=False)

    df_suspended = df_suspended.transpose().rename(index=str, columns={0: 'suspended'})

    return populate_metrics(df_all_instruments, df_conversion_price,
                            df_latest_bond_price, df_latest_stock_price,
                            df_call_info, df_indicators, df_suspended)


def populate_metrics(all_instruments, conversion_price, bond_price,
                     stock_price, call_info, indicators, suspended):
    # Add stock_price column
    stock_price = stock_price[['order_book_id',
                               'close']].rename(columns={
                                   'close': 'stock_price'
                               }).set_index('order_book_id')
    df = all_instruments.set_index('stock_code').join(
        stock_price).reset_index().set_index('order_book_id')

    # Add bond_price column
    bond_price = bond_price[['order_book_id',
                             'close']].rename(columns={
                                 'close': 'bond_price'
                             }).set_index('order_book_id')
    df = df.join(bond_price)

    # Add conversion_price column
    conversion_price = conversion_price[['order_book_id', 'conversion_price'
                                         ]].groupby('order_book_id').min()
    df = df.join(conversion_price)

    # Add info_date column
    df = df.join(call_info.set_index('order_book_id')[['info_date']])

    # Add columns from indicators
    df = df.join(indicators.set_index('order_book_id'))

    # Add suspended column
    df = df.join(suspended)
    return df


def auth(username, password):
    rqdatac.init(username, password)
