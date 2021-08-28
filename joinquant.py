import numpy as np
import os
import pandas as pd
from datetime import date, timedelta
from collections.abc import Callable


# Code to run on joinquant
# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001,
                             open_commission=0.0003,
                             close_commission=0.0003,
                             min_commission=5),
                   type='stock')

    g.top = 20

    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
    # 开盘前运行
    # run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
    # 开盘时运行
    run_daily(market_open, time='open', reference_security='000300.XSHG')
    # 收盘后运行
    # run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')


## 开盘时运行函数
def market_open(context):
    if context.current_dt.weekday() != 4:
        return
    log.info('market_open: Today is Friday, adjust holdings...')
    # 给微信发送消息（添加模拟交易，并绑定微信生效）
    # send_message('今天调仓')

    df_date, df_basic_info, df_latest_bond_price, df_latest_stock_price, df_convert_price_adjust = fetch_jqdata(
    )
    log.info('Using latest jqdata from date: %s' %
             df_date.strftime('%Y-%m-%d'))
    df = massage_jqdata(df_basic_info, df_latest_bond_price,
                        df_latest_stock_price, df_convert_price_adjust)
    candidates = execute_strategy(df, double_low, {
        'weight_bond_price': 0.5,
        'weight_convert_premium_rate': 0.5,
        'top': g.top,
    })
    log.info('Candidates:\n%s' % candidates[[
        'code', 'short_name', 'bond_price', 'convert_premium_rate',
        'double_low'
    ]])
    orders = generate_orders(set(),
                             set(g.candidates.reset_index().code.tolist()))
    execute_orders(orders)


def execute_orders(orders: dict[str, set[str]]):
    for code in orders['sell']:
        security = g.candidates.loc[code]
        log.info('Selling %s %s' % (code, security.short_name))
        order_target(code, 0)

    for code in orders['hold']:
        security = g.candidates.loc[code]
        log.info('Holding %s %s' % (code, security.short_name))
        order_target_value(code, g.portfolio.total_value / g.top)

    for code in orders['buy']:
        security = g.candidates.loc[code]
        log.info('Buying %s %s' % (code, security.short_name))
        order_target_value(code, g.portfolio.total_value / g.top)
