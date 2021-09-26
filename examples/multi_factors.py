# -*- coding: utf-8 -*-
import pandas as pd
from conbond import ricequant, strategy
from rqalpha.api import *
import csv
import pprint


# A few note for this to work:
# convertible bond is not supported by rqalpha by default
# for the backtest to work, we are making the convertible bond as common stock
# The instruments.pk file will need to be updated to include all the bonds' order_book_id
def init(context):
    context.cblogger = open(context.run_dir.joinpath('log.txt'), 'w')
    context.top = 20
    context.ordersf = open(context.run_dir.joinpath('orders.csv'), 'w')
    context.orders = csv.writer(context.ordersf)
    context.orders.writerow(
        ['symbol', 'side', 'positionEffect', 'price', 'volume', 'createdAt'])
    scheduler.run_weekly(rebalance,
                         tradingday=1,
                         time_rule=market_open(minute=10))


def rebalance(context, bar_dict):
    logger.info('Running date: %s' % context.now)
    all_instruments = ricequant.fetch(context.now,
                                      cache_dir=context.cache_dir,
                                      logger=logger)

    df = strategy.rq_filter_conbond(context.now, all_instruments)
    df_candidates = strategy.multi_factors(
        df, {
            'factors': {
                'bond_price': 0.0,
                'conversion_premium': 1.0,
            },
            'top': context.top,
        })
    context.cblogger.write('\n\nCandidates at %s:' % context.now)
    context.cblogger.write('\n%s' % df_candidates[[
        'symbol', 'bond_price', 'conversion_premium', '__rank__'
    ]].to_string())

    candidates = set(df_candidates.index.values.tolist())
    positions = set()
    for p in context.portfolio.get_positions():
        positions.add(p.order_book_id)
    orders = []
    # 平仓
    for order_book_id in list(positions - candidates):
        orders.append(order_target_percent(order_book_id, 0))
    # 调仓
    for order_book_id in list(positions & candidates):
        orders.append(order_target_percent(order_book_id, 1 / context.top))
    # 开仓
    for order_book_id in list(candidates - positions):
        orders.append(order_target_percent(order_book_id, 1 / context.top))

    for order in orders:
        if order is not None:
            #  logging.info(order)
            if order.status != ORDER_STATUS.FILLED:
                context.cblogger.write('\nOrder error: %s' % order)
            context.orders.writerow([
                'SZSE.%s' % order.order_book_id[:6]
                if order.order_book_id.endswith('XSHE') else 'SHSE.%s' %
                order.order_book_id[:6], 1 if order.side == SIDE.BUY else 2,
                1 if order.position_effect == POSITION_EFFECT.OPEN else 2,
                str(order.avg_price),
                str(order.filled_quantity),
                str(order.datetime)
            ])
    context.cblogger.flush()
    context.ordersf.flush()
    #  logging.info(pprint.pformat(context.portfolio))
