#!/usr/bin/env python3

import os
import json
import pandas as pd
import pprint
from absl import app, flags, logging
from datetime import date, timedelta

import jqdatasdk as jqdata
from lib import conbond

FLAGS = flags.FLAGS

flags.DEFINE_bool("use_cache", False, "Use cache or not")
flags.DEFINE_string("cache_dir", None, "Cache directory")
flags.DEFINE_integer("top", 20, "Number of candidates")
flags.DEFINE_string("data_source", "jqdata",
                    "Data source: jqdata, jisilu")


def main(argv):
    df_date = None  # Date of the price information
    df = None

    if FLAGS.data_source == 'jqdata':
        df_basic_info = None  # Basic information of the bond
        df_latest_bond_price = None  # Latest price of the bond
        df_latest_stock_price = None  # Latest price of the stock
        df_convert_price_adjust = None  # Latest convert price of the bond
        if not FLAGS.use_cache:
            jqconfig = json.load(open('jqconfig.json'))
            jqdata.auth(jqconfig['username'], jqconfig['password'])
            df_date, df_basic_info, df_latest_bond_price, df_latest_stock_price, df_convert_price_adjust = conbond.fetch_jqdata(
                jqdata, date.today())
            logging.info('Fetch latest jqdata')
            cache_jqdata(FLAGS.cache_dir, df_basic_info, df_latest_bond_price,
                         df_latest_stock_price, df_convert_price_adjust)
        df_date, df_basic_info, df_latest_bond_price, df_latest_stock_price, df_convert_price_adjust = fetch_jqcache(
            FLAGS.cache_dir)
        logging.info('Using data from date: %s' % df_date.strftime('%Y-%m-%d'))
        df = conbond.massage_jqdata(df_basic_info, df_latest_bond_price,
                                    df_latest_stock_price,
                                    df_convert_price_adjust)
    elif FLAGS.data_source == 'jisilu':
        if not FLAGS.use_cache:
            # Created with
            # var A397151C04723421F = '397151C04723421F';
            # jslencode('user_name', A397151C04723421F)
            # same for password
            jisilu = json.load(open('jisilu.json'))
            df_date, data = conbond.fetch_jisilu(jisilu['user_name'], jisilu['password'])
            logging.info('Fetching latest jisilu data')
            cache_jisilu(FLAGS.cache_dir, data)
        df_date, data = fetch_jisilucache(FLAGS.cache_dir)
        logging.info('Using jisilu data from date: %s' %
                     df_date.strftime('%Y-%m-%d'))
        df = conbond.massage_jisiludata(data)
    else:
        raise

    logging.info('Using double_low strategy')
    candidates, orders = conbond.generate_candidates(
        df, conbond.double_low, {
            'weight_bond_price': 0.5,
            'weight_convert_premium_rate': 0.5,
            'top': FLAGS.top,
        }, set())
    logging.info('Candidates:\n%s' % candidates[[
        'code', 'short_name', 'bond_price', 'convert_premium_rate',
        'double_low'
    ]])
    pprint.pprint(orders)


def fetch_jqcache(cache_dir):
    df_basic_info = pd.read_excel(os.path.join(cache_dir, 'basic_info.xlsx'))
    df_latest_bond_price = pd.read_excel(
        os.path.join(cache_dir, 'latest_bond_price.xlsx'))
    df_latest_stock_price = pd.read_excel(
        os.path.join(cache_dir, 'latest_stock_price.xlsx'))
    df_convert_price_adjust = pd.read_excel(
        os.path.join(cache_dir, 'convert_price_adjust.xlsx'))
    return df_latest_bond_price.date[
        0], df_basic_info, df_latest_bond_price, df_latest_stock_price, df_convert_price_adjust


def cache_jqdata(cache_dir, df_basic_info, df_latest_bond_price,
                 df_latest_stock_price, df_convert_price_adjust):
    df_basic_info.to_excel(os.path.join(cache_dir, 'basic_info.xlsx'))
    df_latest_bond_price.to_excel(
        os.path.join(cache_dir, 'latest_bond_price.xlsx'))
    df_latest_stock_price.to_excel(
        os.path.join(cache_dir, 'latest_stock_price.xlsx'))
    df_convert_price_adjust.to_excel(
        os.path.join(cache_dir, 'convert_price_adjust.xlsx'))


def fetch_jisilucache(cache_dir):
    cache = open(os.path.join(cache_dir, 'jisilu.json'), 'r', encoding='utf-8')
    return date.today() - timedelta(days=1), cache.read()


def cache_jisilu(cache_dir, data):
    cache = open(os.path.join(cache_dir, 'jisilu.json'), 'w', encoding='utf-8')
    cache.write(data)
    cache.close()


if __name__ == "__main__":
    app.run(main)
