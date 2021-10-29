#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from absl import app, flags
from attrdict import AttrDict
from conbond import ricequant, strategy
from datetime import datetime
from rqalpha import run_func
from rqalpha.api import *
import glob
import json
import logging
import pandas as pd
import pathlib

FLAGS = flags.FLAGS

flags.DEFINE_string('strategy_cfg', None, 'Strategy config file')
flags.mark_flag_as_required('strategy_cfg')
flags.DEFINE_string('cache_dir', None, 'Cache directory')
flags.mark_flag_as_required('cache_dir')
flags.DEFINE_string('start_date', None, 'Backtest start date')
flags.mark_flag_as_required('start_date')
flags.DEFINE_string('end_date', None, 'Backtest end date')
flags.mark_flag_as_required('end_date')


# A few note for this to work:
# convertible bond is not supported by rqalpha by default
# for the backtest to work, we are making the convertible bond as common stock
# The instruments.pk file will need to be updated to include all the bonds' order_book_id
def init(context):
    scheduler.run_weekly(rebalance,
                         tradingday=1,
                         time_rule=market_open(minute=10))
    context.written = False
    context.candidatesf = pathlib.Path(context.run_dir).joinpath(
        '%s.csv' % context.strategy_name)


def rebalance(context, bar_dict):
    logger.info('Rebalance date: %s' % context.now)
    df = ricequant.fetch(context.now,
                         cache_dir=context.cache_dir,
                         logger=logging)
    score_col = 'weighted_score'
    rank_col = 'rank'
    s = getattr(strategy, context.strategy_config['scoring_fn'])
    df = s(df, context.now,
           context.strategy_config['config'].convert_to_dict(), score_col,
           rank_col)
    df['date'] = context.now.date()

    positions = set()
    suspended = set()
    for p in context.portfolio.get_positions():
        inst = df.loc[p.order_book_id]
        if inst.at['suspended']:
            suspended.add(p.order_book_id)
            logging.info('持仓停牌: %s' % p.order_book_id)
        else:
            positions.add(p.order_book_id)

    top = context.strategy_config['top']
    head = df[~df.filtered].iloc[top - len(suspended)].at[rank_col]
    df = df.head(head)
    if context.written:
        df[[
            'date', 'symbol', 'bond_price', 'conversion_premium', score_col,
            rank_col, 'filtered', 'filtered_reason'
        ]].to_csv(context.candidatesf, mode='a', header=False, index=True)
    else:
        df[[
            'date', 'symbol', 'bond_price', 'conversion_premium', score_col,
            rank_col, 'filtered', 'filtered_reason'
        ]].to_csv(context.candidatesf, mode='w', header=True, index=True)
        context.written = True

    candidates = set(df[~df.filtered].index.values.tolist())
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


def plot_results(results, savefile=None):
    from matplotlib import rcParams, gridspec, ticker, image as mpimg, pyplot as plt
    from matplotlib.font_manager import findfont, FontProperties
    import numpy as np

    rcParams['font.family'] = 'sans-serif'
    rcParams['font.sans-serif'] = [
        u'Microsoft Yahei',
        u'Heiti SC',
        u'Heiti TC',
        u'STHeiti',
        u'WenQuanYi Zen Hei',
        u'WenQuanYi Micro Hei',
        u'文泉驿微米黑',
        u'SimHei',
    ] + rcParams['font.sans-serif']
    rcParams['axes.unicode_minus'] = False

    use_chinese_fonts = True
    font = findfont(FontProperties(family=['sans-serif']))
    if '/matplotlib/' in font:
        use_chinese_fonts = False
        logging.warn('Missing Chinese fonts. Fallback to English.')

    title = 'Conbond Strateties Comparison'
    benchmark_portfolio = None
    start_date = None
    end_date = None
    plt.style.use('ggplot')
    img_width = 16
    img_height = 10
    fig = plt.figure(title, figsize=(img_width, img_height))
    gs = gridspec.GridSpec(img_height, img_width)
    ax = plt.subplot(gs[2:img_height, :])
    ax.get_xaxis().set_minor_locator(ticker.AutoMinorLocator())
    ax.get_yaxis().set_minor_locator(ticker.AutoMinorLocator())
    ax.grid(b=True, which='minor', linewidth=.2)
    ax.grid(b=True, which='major', linewidth=1)
    table_data = {}
    table_columns = [
        'sharpe', 'max_drawdown', 'total_returns', 'annualized_returns'
    ]
    for strategy, result_dict in results.items():
        summary = result_dict['summary']

        if benchmark_portfolio is None:
            benchmark_portfolio = result_dict.get('benchmark_portfolio')
            start_date = result_dict.get('summary')['start_date']
            end_date = result_dict.get('summary')['end_date']
            index = benchmark_portfolio.index
            portfolio_value = benchmark_portfolio.unit_net_value
            xs = portfolio_value.values
            rt = benchmark_portfolio.unit_net_value.values
            max_dd_end = np.argmax(np.maximum.accumulate(xs) / xs)
            if max_dd_end == 0:
                max_dd_end = len(xs) - 1
            max_dd_start = np.argmax(xs[:max_dd_end]) if max_dd_end > 0 else 0
            max_dd = (
                benchmark_portfolio.loc[index[max_dd_start]].unit_net_value -
                benchmark_portfolio.loc[index[max_dd_end]].unit_net_value
            ) / benchmark_portfolio.loc[index[max_dd_start]].unit_net_value
            ax.plot(benchmark_portfolio['unit_net_value'] - 1.0,
                    label='HS300',
                    alpha=1,
                    linewidth=2)
            table_data['HS300'] = [
                0, max_dd, summary['benchmark_total_returns'],
                summary['benchmark_annualized_returns']
            ]
        table_data[strategy] = [summary[col] for col in table_columns]

        portfolio = result_dict['portfolio']
        ax.plot(portfolio['unit_net_value'] - 1.0,
                label=strategy,
                alpha=1,
                linewidth=2)

    # place legend
    leg = plt.legend(loc='best')
    leg.get_frame().set_alpha(0.5)

    # manipulate axis
    vals = ax.get_yticks()
    ax.set_yticklabels(['{:3.2f}%'.format(x * 100) for x in vals])

    df = pd.DataFrame.from_dict(table_data,
                                orient='index',
                                columns=table_columns).reset_index().rename(
                                    columns={'index': 'strategy'})
    df[['max_drawdown', 'total_returns', 'annualized_returns'
        ]] = df[['max_drawdown', 'total_returns',
                 'annualized_returns']].applymap('{0:.2%}'.format)
    ax2 = plt.subplot(gs[0:2, :])
    ax2.set_title(title)
    ax2.text(0, 1, 'Start Date: %s, End Date: %s' % (start_date, end_date))
    ax2.table(cellText=df.values, colLabels=df.columns, loc='center')
    ax2.axis('off')
    #  ax2.axis('tight')

    if savefile:
        plt.savefig(savefile, bbox_inches='tight')


def backtest(sc, run_dir, cache_dir):
    p = pathlib.Path(sc)
    print(p)
    logging.info('Strategy name: %s' % p.stem)
    cfg = json.load(p.open())
    config = {
        'base': {
            'start_date': FLAGS.start_date,
            'end_date': FLAGS.end_date,
            'accounts': {
                'stock': 1000000
            },
            'frequency': '1d',
            'benchmark': '000300.XSHG',
        },
        'extra': {
            'context_vars': {
                'run_dir': run_dir.resolve(),
                'cache_dir': cache_dir,
                'strategy_config': cfg,
                'strategy_name': p.stem,
            },
            'log_level': 'error',
        },
        'mod': {
            'sys_analyser': {
                'enabled': True,
                'output_file': '%s/%s.pkl' % (run_dir, p.stem),
                'plot': False,
                'plot_save_file': '%s/%s.png' % (run_dir, p.stem),
            },
            'sys_simulation': {
                'enabled': True,
                # 'matching_type': 'last'
            },
            'sys_accounts': {
                'enabled': True,
                # conbond is T0
                'stock_t1': False,
            },
            'sys_scheduler': {
                'enabled': True,
            },
            'sys_progress': {
                'enabled': True,
                'show': True,
            },
            'sys_transaction_cost': {
                'enabled': True,
            },
            'incremental': {
                'enabled': False,
                'strategy_id': 'low_cpr',
                # 是否启用 csv 保存 feeds 功能，可以设置为 MongodbRecorder
                'recorder': 'CsvRecorder',
                # 持久化数据输出文件夹
                'persist_folder': 'cache',
            },
            'local_source': {
                'enabled':
                True,
                'lib':
                'rqalpha_mod_local_source',
                # 其他配置参数
                'start_date':
                '2018-01-02',
                'end_date':
                '2021-09-24',
                'data_path':
                pathlib.Path(__file__).parent.joinpath(cache_dir, 'rqdata',
                                                       'combined.csv'),
                'data_format':
                'csv',
            }
        }
    }
    return p.stem, run_func(init=init, config=config)


def main(argv):
    run_dir = pathlib.Path('logs').joinpath(
        datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
    print('Run dir: {0}, log: {0}/debug.log'.format(run_dir))
    run_dir.mkdir(parents=True, exist_ok=False)
    logging.basicConfig(level=logging.INFO,
                        filename='%s/debug.log' % run_dir,
                        filemode='w',
                        force=True)

    results = {}
    for sc in glob.glob(FLAGS.strategy_cfg):
        k, r = backtest(sc, run_dir, FLAGS.cache_dir)
        results[k] = r['sys_analyser']
    print('Run dir: {0}, log: {0}/debug.log'.format(run_dir))
    plot_results(results, savefile='results.png')


if __name__ == '__main__':
    app.run(main)
