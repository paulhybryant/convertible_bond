#!/usr/bin/env python3

import os
import json
import pandas as pd
import pprint
from absl import app, flags, logging
from datetime import date, timedelta
import jqdatasdk as jqdata
from lib import conbond
import execjs
import pathlib

FLAGS = flags.FLAGS

flags.DEFINE_bool("use_cache", False, "Use cache or not")
flags.DEFINE_string("cache_dir", None, "Cache directory")
flags.DEFINE_integer("top", 20, "Number of candidates")
flags.DEFINE_string("data_source", "jqdata", "Data source: jqdata, jisilu")
flags.DEFINE_string("positions", "positions.json", "File to store positions")
flags.DEFINE_string("txn_day",
                    date.today().strftime('%Y-%m-%d'),
                    "Date to generate the candidates")


def main(argv):
    df_date = None  # Date of the price information
    df = None

    if FLAGS.data_source == 'jqdata':
        if not FLAGS.use_cache:
            auth_file = pathlib.Path('auth.json')
            if not auth_file.exists():
                logging.fatal(
                    'To get data from jqdata, need to put credential in auth.json under "jqdata" with key "username" and "password"'
                )
            auth = json.load(auth_file.open('r'))
            assert 'jqdata' in auth
            assert 'username' in auth['jqdata']
            assert 'password' in auth['jqdata']
            jqdata.auth(auth['jqdata']['username'], auth['jqdata']['password'])
        today = date.fromisoformat(FLAGS.txn_day)
        df_date, df = conbond.fetch_jqdata(jqdata, today, FLAGS.cache_dir,
                                           FLAGS.use_cache)
        assert df_date < today, 'Cached data should be older than --txn_day'
        logging.info('Using data from date: %s' % df_date.strftime('%Y-%m-%d'))
    elif FLAGS.data_source == 'jisilu':
        # Created with
        # var A397151C04723421F = '397151C04723421F';
        # jslencode('username', A397151C04723421F)
        # same for password
        auth_file = pathlib.Path('auth.json')
        if not auth_file.exists():
            logging.fatal(
                'To get data from jisilu, need to put credential in auth.json under "jisilu" with key "username" and "password"'
            )
        auth = json.load(auth_file.open('r'))
        assert 'jisilu' in auth
        assert 'username' in auth['jisilu']
        assert 'password' in auth['jisilu']
        with open('jisilu.js', 'r', encoding='utf8') as f:
            source = f.read()
        ctx = execjs.compile(source)
        username = ctx.call('jslencode', auth['jisilu']['username'],
                            '397151C04723421F')
        password = ctx.call('jslencode', auth['jisilu']['password'],
                            '397151C04723421F')
        df_date, df = conbond.fetch_jisilu(username, password, FLAGS.cache_dir,
                                           FLAGS.use_cache)
        logging.info('Using jisilu data from date: %s' %
                     df_date.strftime('%Y-%m-%d'))
    else:
        raise

    positions_file = pathlib.Path(FLAGS.positions)
    if positions_file.exists():
        positions = json.load(positions_file.open('r'))
    else:
        positions = json.loads(
            '{"current": "NONE", "NONE": {"positions": [], "orders": {}}}')

    logging.info('Using double_low strategy')
    candidates, orders = conbond.generate_candidates(
        df, conbond.double_low, {
            'weight_bond_price': 0.5,
            'weight_convert_premium_rate': 0.5,
            'top': FLAGS.top,
        }, set(positions[positions['current']]['positions']))
    logging.info('Candidates:\n%s' % candidates[[
        'code', 'short_name', 'bond_price', 'convert_premium_rate',
        'double_low'
    ]])
    for k, v in orders.items():
        logging.info('%s: %s' % (k, v))
    confirm = input('Update positions (y/n)? ')
    if confirm == 'y':
        logging.info('Updating positions')
        positions['current'] = df_date.strftime('%Y-%m-%d')
        positions[positions['current']] = {}
        positions[positions['current']]['positions'] = candidates.code.tolist()
        positions[positions['current']]['orders'] = orders
        positions_file.open('w').write(json.dumps(positions))
    else:
        logging.info('Positions is not updated.')


if __name__ == "__main__":
    app.run(main)
