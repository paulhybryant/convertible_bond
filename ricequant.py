# -*- coding: utf-8 -*-
# 导入函数库
import rqdatac
import conbond


# 初始化函数，设定基准等等
def init(context):
    scheduler.run_weekly(rebalance, tradingday=1)


def handle_bar(context, bar_dict):
    pass


def rebalance(context, bar_dict):
    df_date, df = conbond.fetch_rqdata(rqdata, context.now.date())
    log.info('Using latest jqdata from date: %s' %
             df_date.strftime('%Y-%m-%d'))
    # TODO: pass actual holdings from context
    candidates, orders = conbond.generate_candidates(
        df, conbond.double_low, {
            'weight_bond_price': 0.5,
            'weight_convert_premium_rate': 0.5,
            'top': g.top,
        }, set())
    log.info('Candidates:\n%s' % candidates[[
        'code', 'short_name', 'bond_price', 'convert_premium_rate',
        'double_low'
    ]])
    execute_orders(orders, context.portfolio)


def execute_orders(orders, portfolio):
    for code in orders['sell']:
        order_target(code, 0)

    for op in ['hold', 'buy']:
        for code in orders[op]:
            order_target_value(code, portfolio.total_value / g.top)
