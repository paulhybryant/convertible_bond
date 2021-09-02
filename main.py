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
import rqdatac

FLAGS = flags.FLAGS

flags.DEFINE_bool("use_cache", False, "Use cache or not")
flags.DEFINE_string("cache_dir", None, "Cache directory")
flags.DEFINE_integer("top", 20, "Number of candidates")
flags.DEFINE_string("data_source", "jqdata",
                    "Data source: jqdata, jisilu, rqdata")
flags.DEFINE_string("positions", "positions.json", "File to store positions")
flags.DEFINE_string("txn_day",
                    date.today().strftime('%Y-%m-%d'),
                    "Date to generate the candidates")


def main(argv):
    df_date = None  # Date of the price information
    df = None
    username = None
    password = None

    if not FLAGS.use_cache:
        auth_file = pathlib.Path('auth.json')
        if not auth_file.exists():
            logging.fatal('auth.json is missing, see README.md')
        auth = json.load(auth_file.open('r'))
        assert FLAGS.data_source in auth
        assert 'username' in auth[FLAGS.data_source]
        assert 'password' in auth[FLAGS.data_source]
        username = auth[FLAGS.data_source]['username']
        password = auth[FLAGS.data_source]['password']

    if FLAGS.data_source == 'jqdata':
        if not FLAGS.use_cache:
            jqdata.auth(username, password)
        df_date, df = conbond.fetch_jqdata(jqdata,
                                           date.fromisoformat(FLAGS.txn_day),
                                           FLAGS.cache_dir, FLAGS.use_cache)
    elif FLAGS.data_source == 'jisilu':
        with open('jisilu.js', 'r', encoding='utf8') as f:
            source = f.read()
        key = '397151C04723421F'
        ctx = execjs.compile(source)
        username = ctx.call('jslencode', username, key)
        password = ctx.call('jslencode', password, key)
        df_date, df = conbond.fetch_jisilu(username, password, FLAGS.cache_dir,
                                           FLAGS.use_cache)
    elif FLAGS.data_source == 'rqdata':
        if not FLAGS.use_cache:
            rqdatac.init(username, password)
        df_date, df = conbond.fetch_rqdata(rqdatac,
                                           date.fromisoformat(FLAGS.txn_day),
                                           FLAGS.cache_dir, FLAGS.use_cache)
    else:
        raise

    logging.info('Using %s data from date: %s' %
                 (FLAGS.data_source, df_date.strftime('%Y-%m-%d')))
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
    logging.info('Candidates:\n%s' % candidates)
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
