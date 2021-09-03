# -*- coding: utf-8 -*-
import rqdatac
from datetime import date
from rqalpha.api import *
import os


def init(context):
    # rqdatac.init('license', '')
    context.top = 20
    scheduler.run_weekly(before_trading,
                         tradingday=1,
                         time_rule='before_trading')
    #  scheduler.run_weekly(rebalance,
                         #  tradingday=1,
                         #  time_rule=market_open(minute=10))


def rebalance(context, bar_dict):
    for code in context.orders['sell']:
        order_target_precent(code, 0)
    for op in ['hold', 'buy']:
        for code in context.orders[op]:
            order_target_percent(code, 1 / 20)


def before_trading(context, bar_dict):
    #  logger.info(context.now.strftime('%Y-%m-%d'))
    cache_dir = os.path.join('~/.conbond', context.now.strftime('%Y-%m-%d'))
    df_date, df = fetch_rqdata(rqdatac, context.now, cache_dir, True)
    positions = set()
    for p in context.portfolio.get_positions():
        positions.add(p.order_book_id)
    candidates, orders = generate_candidates(
        df, double_low, {
            'weight_bond_price': 0.5,
            'weight_convert_premium_rate': 0.5,
            'top': context.top,
        }, positions)
    context.orders = orders


def fetch_rqdata(rqdatac, today, cache_dir, use_cache):
    df_basic_info = None
    df_latest_bond_price = None
    df_latest_stock_price = None
    df_convert_price_adjust = None
    txn_day = None

    if use_cache:
        assert cache_dir
        df_basic_info = pd.read_excel(
            os.path.join(cache_dir, 'rq_basic_info.xlsx'))
        df_latest_bond_price = pd.read_excel(
            os.path.join(cache_dir, 'rq_latest_bond_price.xlsx'))
        df_latest_stock_price = pd.read_excel(
            os.path.join(cache_dir, 'rq_latest_stock_price.xlsx'))
        df_convert_price_adjust = pd.read_excel(
            os.path.join(cache_dir, 'rq_convert_price_adjust.xlsx'))
        txn_day = df_latest_bond_price.date[0]
        assert txn_day < today, 'Cached data should be older than %s' % today
    else:
        txn_day = rqdatac.get_previous_trading_date(today)
        df_basic_info = rqdatac.convertible.all_instruments(
            txn_day).reset_index()
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
            df_basic_info.order_book_id.tolist(),
            end_date=txn_day).reset_index()

        if cache_dir:
            df_basic_info.to_excel(
                os.path.join(cache_dir, 'rq_basic_info.xlsx'))
            df_latest_bond_price.to_excel(
                os.path.join(cache_dir, 'rq_latest_bond_price.xlsx'))
            df_latest_stock_price.to_excel(
                os.path.join(cache_dir, 'rq_latest_stock_price.xlsx'))
            df_convert_price_adjust.to_excel(
                os.path.join(cache_dir, 'rq_convert_price_adjust.xlsx'))

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


# config: Expect to have two keys: weight_bond_price and weight_convert_premium_rate
# df: Expect to have a column named 'double_low', or two columns named 'bond_price' and 'convert_premium_rate'
def double_low(df, config):
    assert 'top' in config
    top = config['top']

    dl_df = None
    if 'double_low' in df.columns:
        dl_df = df
    else:
        dl_df = df
        assert 'weight_bond_price' in config
        assert 'weight_convert_premium_rate' in config
        weight_bond_price = config['weight_bond_price']
        weight_convert_premium_rate = config['weight_convert_premium_rate']
        assert 'bond_price' in dl_df.columns
        assert 'convert_premium_rate' in dl_df.columns
        dl_df[
            'double_low'] = dl_df.bond_price * weight_bond_price + dl_df.convert_premium_rate * 100 * weight_convert_premium_rate
    return dl_df.nsmallest(top, 'double_low')


def generate_candidates(df, strategy, strategy_config, holdings):
    candidates = strategy(df, strategy_config)
    candidate_codes = set(candidates.code.tolist())
    orders = {}
    orders['buy'] = list(candidate_codes - holdings)
    orders['sell'] = list(holdings - candidate_codes)
    orders['hold'] = list(holdings & candidate_codes)
    return candidates, orders
