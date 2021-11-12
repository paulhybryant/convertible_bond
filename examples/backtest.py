#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from absl import app, flags
from attrdict import AttrDict
from conbond import ricequant, strategy
from datetime import datetime
from rqalpha import run_func
from rqalpha.api import logger
import glob
import json
import pandas as pd
import pathlib
from rqalpha.utils.logger import user_log
from logbook import Logger, FileHandler

FLAGS = flags.FLAGS

flags.DEFINE_string('strategy_cfg', None, 'Strategy config file')
flags.mark_flag_as_required('strategy_cfg')
flags.DEFINE_string('cache_dir', None, 'Cache directory')
flags.mark_flag_as_required('cache_dir')
flags.DEFINE_string('start_date', None, 'Backtest start date')
flags.mark_flag_as_required('start_date')
flags.DEFINE_string('end_date', None, 'Backtest end date')
flags.mark_flag_as_required('end_date')
flags.DEFINE_string('results', 'results.png', 'results plot file')
flags.DEFINE_string('run_dir', None, 'Run directory')


# A few note for this to work:
# convertible bond is not supported by rqalpha by default
# for the backtest to work, we are making the convertible bond as common stock
# The instruments.pk file will need to be updated to include all the bonds' order_book_id
def init(context):
    #  scheduler.run_weekly(rebalance, tradingday=1)
    scheduler.run_daily(rebalance)


def rebalance(context, bar_dict):
    logger.info('Rebalance date: %s' % context.now)
    df = ricequant.fetch(context.now,
                         cache_dir=context.cache_dir,
                         logger=logger)
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
            context.logf.info('持仓停牌: %s' % p.order_book_id)
        else:
            positions.add(p.order_book_id)

    top = context.strategy_config['top']
    head = df[~df.filtered].iloc[top - len(suspended)].at[rank_col]
    df = df.head(head)
    if hasattr(context, 'candidatesf'):
        df[[
            'date', 'symbol', 'bond_price', 'conversion_premium', score_col,
            rank_col, 'filtered', 'filtered_reason'
        ]].to_csv(context.candidatesf, mode='a', header=False, index=True)
    else:
        context.candidatesf = pathlib.Path(context.run_dir).joinpath(
            '%s.csv' % context.strategy_name)
        df[[
            'date', 'symbol', 'bond_price', 'conversion_premium', score_col,
            rank_col, 'filtered', 'filtered_reason'
        ]].to_csv(context.candidatesf, mode='w', header=True, index=True)

    candidates = set(df[~df.filtered].index.values.tolist())
    # 平仓
    for order_book_id in (positions - candidates):
        order = order_target_percent(order_book_id, 0)
        if order is not None and order.status != ORDER_STATUS.FILLED:
            context.logf.info('Order error: %s' % order)
    # 调仓
    for order_book_id in (positions & candidates):
        order = order_target_percent(order_book_id, 1 / top)
        if order is not None and order.status != ORDER_STATUS.FILLED:
            context.logf.info('Order error: %s' % order)
    # 开仓
    for order_book_id in (candidates - positions):
        order = order_target_percent(order_book_id, 1 / top)
        if order is not None and order.status != ORDER_STATUS.FILLED:
            context.logf.info('Order error: %s' % order)


def backtest(cfg, run_dir, cache_dir, logger):
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
                'run_dir': run_dir,
                'cache_dir': cache_dir,
                'strategy_config': cfg,
                'strategy_name': cfg['name'],
                'logf': logger,
            },
            'log_level': 'error',
        },
        'mod': {
            'sys_analyser': {
                'enabled': True,
                'output_file': '%s/%s.pkl' % (run_dir, cfg['name']),
                'plot': False,
                'plot_save_file': '%s/%s.png' % (run_dir, cfg['name']),
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
                '2021-11-10',
                'data_path':
                pathlib.Path(__file__).parent.joinpath(cache_dir, 'rqdata',
                                                       'combined.csv'),
                'data_format':
                'csv',
            }
        }
    }
    return run_func(init=init, config=config)


def main(argv):
    backtest_time = datetime.now()
    if FLAGS.run_dir:
        run_dir = pathlib.Path(FLAGS.run_dir).resolve()
    else:
        run_dir = pathlib.Path('logs').joinpath(
            backtest_time.strftime('%Y-%m-%d-%H-%M-%S')).resolve()
        run_dir.mkdir(parents=True, exist_ok=False)
    print('Run dir: {0}'.format(run_dir))
    logf = Logger('backtest')
    fh = FileHandler('%s/debug.log' % run_dir, level='DEBUG', bubble=True)
    fh.push_application()
    results = {}
    for sc in glob.glob(FLAGS.strategy_cfg):
        p = pathlib.Path(sc)
        cfg = json.load(p.open())
        cfg['name'] = p.stem
        r = run_dir.joinpath('%s.pkl' % p.stem)
        if r.exists() and not cfg['force']:
            print('Result for %s exists, skipping' % sc)
            results[cfg['name']] = pd.read_pickle(r)
        else:
            print('Strategy name: %s' % cfg['name'])
            r = backtest(cfg, run_dir, FLAGS.cache_dir, logf)
            results[cfg['name']] = r['sys_analyser']
    print('Run dir: {0}'.format(run_dir))
    strategy.plot_results(backtest_time, results, savefile=FLAGS.results)


if __name__ == '__main__':
    app.run(main)
