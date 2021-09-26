#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from datetime import datetime
from rqalpha import run_file
import pathlib
from absl import app, flags
import logging

FLAGS = flags.FLAGS

flags.DEFINE_string('file', None, 'Strategy file')
flags.mark_flag_as_required('file')
flags.DEFINE_string('cache_dir', None, 'Cache directory')
flags.mark_flag_as_required('cache_dir')
flags.DEFINE_string('start_date', None, 'Backtest start date')
flags.mark_flag_as_required('start_date')
flags.DEFINE_string('end_date', None, 'Backtest end date')
flags.mark_flag_as_required('end_date')


def main(argv):
    run_dir = pathlib.Path('logs').joinpath(
        datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
    logging.info('Run dir: {0}, log: {0}/debug.log'.format(run_dir))
    run_dir.mkdir(parents=True, exist_ok=False)
    logging.basicConfig(level=logging.INFO,
                        filename='%s/debug.log' % run_dir,
                        filemode='w',
                        force=True)
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
                'cache_dir': FLAGS.cache_dir,
            },
            'log_level': 'error',
        },
        'mod': {
            'sys_analyser': {
                'enabled': True,
                'output_file': '%s/result.pkl' % run_dir,
                'report_save_path': '%s/report' % run_dir,
                'plot': True,
                'plot_save_file': '%s/plot.png' % run_dir,
            },
            'sys_simulation': {
                'enabled': True,
                # 'matching_type': 'last'
            },
            'sys_accounts': {
                'enabled': True,
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
                'enabled': True,
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
                '2021-09-23',
                'data_path':
                pathlib.Path(__file__).parent.joinpath(FLAGS.cache_dir,
                                                       'combined.csv'),
                'data_format':
                'csv',
            }
        }
    }
    run_file(FLAGS.file, config)


if __name__ == '__main__':
    app.run(main)
