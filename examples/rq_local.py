# -*- coding: utf-8 -*-
import pandas as pd
from conbond import ricequant
from datetime import date
from rqalpha import run
from rqalpha.api import *

config = {
    "base": {
        "start_date": "2019-01-01",
        "end_date": "2020-01-01",
        "accounts": {
            "stock": 1000000
        },
        "frequency": "1d",
        "benchmark": None,
        "strategy_file": __file__,
    },
    "mod": {
        "sys_analyser": {
            "enabled": True,
            # "report_save_path": ".",
            #  "plot": True
        },
        "sys_simulation": {
            "enabled": True,
            # "matching_type": "last"
        },
        "sys_accounts": {
            "enabled": True,
            # "report_save_path": ".",
            #  "plot": True
        },
        "sys_scheduler": {
            "enabled": True,
            # "report_save_path": ".",
            #  "plot": True
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
            "/Users/yuhuang/gitrepo/convertible_bond/cache/combined.xlsx",
        }
    }
}


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
    logger.info(context.now)
    context.top = 20
    scheduler.run_weekly(rebalance,
                         tradingday=1,
                         time_rule=market_open(minute=10))

    def before_trading(context):
        logger.info('before_trading')

def handle_bar(context, bar_dict):
    logger.info('handle_bar')


def rebalance(context, bar_dict):
    txn_day = get_previous_trading_date(context.now)
    df = ricequant.process(*(ricequant.fetch(txn_day, "/Users/yuhuang/gitrepo/convertible_bond/cache")))
    positions = set()
    for p in context.portfolio.get_positions():
        positions.add(p.order_book_id)
    orders = generate_orders(
        df, double_low, {
            'weight_bond_price': 0.5,
            'weight_convert_premium_rate': 0.5,
            'top': context.top,
        }, positions)
    logger.info("今日操作：%s" % orders)
    for code in orders['sell']:
        order_target_percent(code, 0)
    for op in ['hold', 'buy']:
        for code in orders[op]:
            order_target_percent(code, 1 / context.top)

if __name__ == "__main__":
    # 您可以指定您要传递的参数
    run(config=config)
