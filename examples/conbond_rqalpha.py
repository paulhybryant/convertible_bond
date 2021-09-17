# -*- coding: utf-8 -*-
import pandas as pd
from conbond import ricequant, strategy
from datetime import date
from rqalpha import run
from rqalpha.api import *
import csv

# A few note for this to work:
# convertible bond is not supported by rqalpha by default
# for the backtest to work, we are making the convertible bond as common stock
# The instruments.pk file will need to be updated to include all the bonds' order_book_id

config = {
    "base": {
        "start_date": "2018-01-01",
        "end_date": "2021-09-01",
        "accounts": {
            "stock": 1000000
        },
        "frequency": "1d",
        "benchmark": "000300.XSHG",
        "strategy_file": __file__,
    },
    "extra": {
        "log_level": "error",
    },
    "mod": {
        "sys_analyser": {
            "enabled": True,
            # "report_save_path": ".",
            "plot": True
        },
        "sys_simulation": {
            "enabled": True,
            # "matching_type": "last"
        },
        "sys_accounts": {
            "enabled": True,
            # "report_save_path": ".",
            "plot": True
        },
        "sys_scheduler": {
            "enabled": True,
            # "report_save_path": ".",
            #  "plot": True
        },
        "sys_progress": {
            "enabled": True,
            "show": True,
        },
        "local_source": {
            "enabled":
            True,
            "lib":
            "rqalpha_mod_local_source",
            # 其他配置参数
            "start_date":
            "2018-12-28",
            "end_date":
            "2021-09-08",
            "data_path":
            "/Users/yuhuang/gitrepo/convertible_bond/examples/cache/combined.xlsx",
        }
    }
}


def init(context):
    context.top = 20
    context.ordersf = open('cache/orders.csv', 'w')
    context.orders = csv.writer(context.ordersf)
    context.orders.writerow(
        ['symbol', 'side', 'positionEffect', 'price', 'volume', 'createdAt'])
    scheduler.run_weekly(rebalance,
                         tradingday=1,
                         time_rule=market_open(minute=10))


def rebalance(context, bar_dict):
    logger.info('Running date: %s' % context.now)
    txn_day = get_previous_trading_date(context.now)
    df = ricequant.process(*(ricequant.fetch(
        txn_day, "/Users/yuhuang/gitrepo/convertible_bond/examples/cache", logger)))
    positions = set()
    for p in context.portfolio.get_positions():
        positions.add(p.order_book_id)
    candidates = strategy.double_low(
        df, {
            'weight_bond_price': 0.5,
            'weight_convert_premium_rate': 0.5,
            'top': context.top,
        })

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
            # TODO: Convert to juejin format for analysis
            context.orders.writerow([
                order.order_book_id, str(order.side), str(order.position_effect),
                str(order.avg_price), str(order.filled_quantity), str(order.datetime)
            ])
    context.ordersf.flush()


if __name__ == "__main__":
    run(config=config)
