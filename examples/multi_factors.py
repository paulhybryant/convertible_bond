# -*- coding: utf-8 -*-
import pandas as pd
from conbond import ricequant, strategy
from rqalpha.api import *
import csv
import pprint
import logging


# A few note for this to work:
# convertible bond is not supported by rqalpha by default
# for the backtest to work, we are making the convertible bond as common stock
# The instruments.pk file will need to be updated to include all the bonds' order_book_id
def init(context):
    scheduler.run_weekly(rebalance,
                         tradingday=1,
                         time_rule=market_open(minute=10))


def rebalance(context, bar_dict):
    logger.info('Rebalance date: %s' % context.now)
    top = 20
    all_instruments = ricequant.fetch(context.now,
                                      cache_dir=context.cache_dir,
                                      logger=logging)

    df = strategy.rq_filter_conbond(context.now, all_instruments)
    df_candidates = strategy.multi_factors(
        df, {
            'factors': {
                'bond_price': 0.0,
                'conversion_premium': 1.0,
            },
            'top': top,
        })
    logging.info('Candidates at %s:' % context.now)
    logging.info(df_candidates[[
        'symbol', 'bond_price', 'conversion_premium', '__rank__'
    ]].to_string())

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
        order = order_target_percent(order_book_id, 1 / top)
        if order is not None and order.status != ORDER_STATUS.FILLED:
            logging.info('Order error: %s' % order)
    # 开仓
    for order_book_id in (candidates - positions):
        order = order_target_percent(order_book_id, 1 / top)
        if order is not None and order.status != ORDER_STATUS.FILLED:
            logging.info('Order error: %s' % order)
