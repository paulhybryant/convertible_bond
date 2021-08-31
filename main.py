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

FLAGS = flags.FLAGS

flags.DEFINE_bool("use_cache", False, "Use cache or not")
flags.DEFINE_string("cache_dir", None, "Cache directory")
flags.DEFINE_integer("top", 20, "Number of candidates")
flags.DEFINE_string("data_source", "jqdata", "Data source: jqdata, jisilu")


def main(argv):
    df_date = None  # Date of the price information
    df = None

    if FLAGS.data_source == 'jqdata':
        if not FLAGS.use_cache:
            jqconfig = json.load(open('jqconfig.json'))
            jqdata.auth(jqconfig['username'], jqconfig['password'])
        df_date, df = conbond.fetch_jqdata(jqdata, date.today(),
                                           FLAGS.cache_dir, FLAGS.use_cache)
        logging.info('Using data from date: %s' % df_date.strftime('%Y-%m-%d'))
    elif FLAGS.data_source == 'jisilu':
        # Created with
        # var A397151C04723421F = '397151C04723421F';
        # jslencode('user_name', A397151C04723421F)
        # same for password
        jisilu = json.load(open('jisilu.json'))
        with open('jisilu.js', 'r', encoding='utf8') as f:
            source = f.read()
        ctx = execjs.compile(source)
        user_name = ctx.call('jslencode', jisilu['user_name'],
                             '397151C04723421F')
        password = ctx.call('jslencode', jisilu['password'],
                            '397151C04723421F')
        df_date, df = conbond.fetch_jisilu(user_name, password,
                                           FLAGS.cache_dir, FLAGS.use_cache)
        logging.info('Using jisilu data from date: %s' %
                     df_date.strftime('%Y-%m-%d'))
    else:
        raise

    holdings = set()
    logging.info('Using double_low strategy')
    candidates, orders = conbond.generate_candidates(
        df, conbond.double_low, {
            'weight_bond_price': 0.5,
            'weight_convert_premium_rate': 0.5,
            'top': FLAGS.top,
        }, holdings)
    logging.info('Candidates:\n%s' % candidates[[
        'code', 'short_name', 'bond_price', 'convert_premium_rate',
        'double_low'
    ]])
    pprint.pprint(orders)


if __name__ == "__main__":
    app.run(main)
