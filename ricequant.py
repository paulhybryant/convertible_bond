# -*- coding: utf-8 -*-
from datetime import date
import rqdatac
from rqalpha.api import *


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
        df_basic_info.order_book_id.tolist()).reset_index()
    return txn_day, df_all_instruments, df_conversion_price, df_latest_bond_price, df_latest_stock_price, df_call_info


def process(df_all_instruments, df_conversion_price, df_latest_bond_price,
            df_latest_stock_price, df_call_info):
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

    df = df[df.info_date.dt.date > txn_day]

    df_conversion_price = df_conversion_price[[
        'order_book_id', 'conversion_price'
    ]].groupby('order_book_id').min()

    df = df.set_index('order_book_id').join(df_conversion_price)

    df_latest_stock_price = df_latest_stock_price[[
        'order_book_id', 'close'
    ]].rename(columns={'close': 'stock_price'})
    df = df.reset_index().set_index('stock_code').join(
        df_latest_stock_price.set_index('order_book_id'))

    df['convert_premium_rate'] = df.bond_price / (100 / df.conversion_price *
                                                  df.stock_price) - 1
    return df


# config: Expect to have two keys: weight_bond_price and weight_convert_premium_rate
# df: Expect to have a column named 'double_low', or two columns named 'bond_price' and 'convert_premium_rate'
# index of df is the id for the bond to place order
def double_low(df, config):
    assert 'top' in config
    top = config['top']

    if 'double_low' not in df.columns:
        assert 'weight_bond_price' in config
        assert 'weight_convert_premium_rate' in config
        weight_bond_price = config['weight_bond_price']
        weight_convert_premium_rate = config['weight_convert_premium_rate']
        assert 'bond_price' in df.columns
        assert 'convert_premium_rate' in df.columns
        df['double_low'] = df.bond_price * weight_bond_price + df.convert_premium_rate * 100 * weight_convert_premium_rate
    dl = df.nsmallest(top, 'double_low')
    print(dl)
    return set(df.nsmallest(top, 'double_low').index.values.tolist())


def generate_orders(df, strategy, strategy_config, holdings):
    candidates = strategy(df, strategy_config)
    orders = {}
    orders['buy'] = list(candidates - holdings)
    orders['sell'] = list(holdings - candidates)
    orders['hold'] = list(holdings & candidates)
    return orders


def init(context):
    context.top = 20
    scheduler.run_weekly(rebalance,
                         tradingday=1,
                         time_rule=market_open(minute=10))


def rebalance(context, bar_dict):
    df_date, df_all_instruments, df_conversion_price, df_latest_bond_price, df_latest_stock_price, df_call_info = read_data(
        context.now)
    df = process(df_all_instruments, df_conversion_price, df_latest_bond_price,
                 df_latest_stock_price, df_call_info)
    positions = set()
    for p in context.portfolio.get_positions():
        positions.add(p.order_book_id)
    orders = generate_orders(
        df, double_low, {
            'weight_bond_price': 0.5,
            'weight_convert_premium_rate': 0.5,
            'top': context.top,
        }, positions)
    context.orders = orders
    logger.info("今日操作：%s" % context.orders)
    for code in orders['sell']:
        order_target_percent(code, 0)
    for op in ['hold', 'buy']:
        for code in orders[op]:
            order_target_percent(code, 1 / 20)
