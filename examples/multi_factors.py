# -*- coding: utf-8 -*-
import pandas as pd
from conbond import ricequant, strategy
from rqalpha.api import *
import csv
import pprint
import logging
import pathlib


# A few note for this to work:
# convertible bond is not supported by rqalpha by default
# for the backtest to work, we are making the convertible bond as common stock
# The instruments.pk file will need to be updated to include all the bonds' order_book_id
def init(context):
    scheduler.run_weekly(rebalance,
                         tradingday=1,
                         time_rule=market_open(minute=10))
    context.written = False
    context.candidatesf = pathlib.Path(
        context.run_dir).joinpath('candidates.csv')
    context.filteredf = pathlib.Path(
        context.run_dir).joinpath('filtered.csv')


def rebalance(context, bar_dict):
    logger.info('Rebalance date: %s' % context.now)
    df = ricequant.fetch(context.now,
                         cache_dir=context.cache_dir,
                         logger=logging)
    df = strategy.multi_factors(df, context.strategy_config)
    df = df.sort_values('weighted_score').reset_index()
    df['date'] = context.now.date()
    df['rank'] = df.index.to_series()
    df = df.set_index('order_book_id')

    df_filtered = df[df.filtered][['symbol', 'filtered_reason', 'date', 'rank']]
    df_candidates = df[~df.filtered].head(context.top)
    if context.written:
        df_candidates[[
            'symbol', 'bond_price', 'conversion_premium', 'weighted_score', 'date', 'rank'
        ]].to_csv(context.candidatesf, mode='a', header=False, index=True)
        df_filtered.to_csv(context.filteredf, mode='a', header=False, index=True)
    else:
        df_candidates[[
            'symbol', 'bond_price', 'conversion_premium', 'weighted_score', 'date', 'rank'
        ]].to_csv(context.candidatesf, mode='w', header=True, index=True)
        df_filtered.to_csv(context.filteredf, mode='w', header=True, index=True)
        context.written = True

    candidates = set(df_candidates.index.values.tolist())
    positions = set()
    for p in context.portfolio.get_positions():
        positions.add(p.order_book_id)
    # 平仓
    for order_book_id in (positions - candidates):
        order = order_target_percent(order_book_id, 0)
        if order is not None and order.status != ORDER_STATUS.FILLED:
            logging.info('Order error: %s' % order)
    # 调仓
    for order_book_id in (positions & candidates):
        order = order_target_percent(order_book_id,
                                     1 / context.top)
        if order is not None and order.status != ORDER_STATUS.FILLED:
            logging.info('Order error: %s' % order)
    # 开仓
    for order_book_id in (candidates - positions):
        order = order_target_percent(order_book_id,
                                     1 / context.top)
        if order is not None and order.status != ORDER_STATUS.FILLED:
            logging.info('Order error: %s' % order)
