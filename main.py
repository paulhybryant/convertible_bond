#!/usr/bin/env python3

import os
import json
import pprint
from absl import app, flags, logging
from datetime import date

import jqdatasdk as jqdata
from lib import conbond

FLAGS = flags.FLAGS

flags.DEFINE_bool("use_cache", False, "Use cached data")
flags.DEFINE_string("cache_dir", None, "Cache directory")
flags.DEFINE_integer("top", 20, "Number of candidates")
flags.DEFINE_string("data_source", "jqdata", "Data source: JQData, jisilu")


def main(argv):
    df_basic_info = None  # Basic information of the bond
    df_latest_bond_price = None  # Latest price of the bond
    df_latest_stock_price = None  # Latest price of the stock
    df_convert_price_adjust = None  # Latest convert price of the bond
    df_date = None  # Date of the price information

    if FLAGS.use_cache:
        df_date, df_basic_info, df_latest_bond_price, df_latest_stock_price, df_convert_price_adjust = conbond.fetch_cache(
            FLAGS.cache_dir)
        logging.info('Using cached data from date: %s' %
                     df_date.strftime('%Y-%m-%d'))
    else:
        if FLAGS.data_source == 'jqdata':
            jqconfig = json.load(open('jqconfig.json'))
            jqdata.auth(jqconfig['username'], jqconfig['password'])
            df_date, df_basic_info, df_latest_bond_price, df_latest_stock_price, df_convert_price_adjust = conbond.fetch_jqdata(
                jqdata, date.today())
            if FLAGS.cache_dir:
                df_basic_info.to_excel(
                    os.path.join(FLAGS.cache_dir, 'basic_info.xlsx'))
                df_latest_bond_price.to_excel(
                    os.path.join(FLAGS.cache_dir, 'latest_bond_price.xlsx'))
                df_latest_stock_price.to_excel(
                    os.path.join(FLAGS.cache_dir, 'latest_stock_price.xlsx'))
                df_convert_price_adjust.to_excel(
                    os.path.join(FLAGS.cache_dir, 'convert_price_adjust.xlsx'))
            logging.info('Using latest jqdata from date: %s' %
                         df_date.strftime('%Y-%m-%d'))
        else:
            # TODO: get data from jisilu
            pass

    df = conbond.massage_data(df_basic_info, df_latest_bond_price,
                              df_latest_stock_price, df_convert_price_adjust)
    logging.info('Using double_low strategy')
    candidates = conbond.execute_strategy(
        df, conbond.double_low, {
            'weight_bond_price': 0.5,
            'weight_convert_premium_rate': 0.5,
            'top': FLAGS.top,
        })
    candidates['code'] = candidates[['code', 'exchange_code']].agg('.'.join, axis=1)
    logging.info('Candidates:\n%s' % candidates[[
        'code', 'short_name', 'bond_price', 'convert_premium_rate',
        'double_low'
    ]])
    orders = conbond.generate_orders(set(), set(candidates.code.tolist()))
    pprint.pprint(orders)


if __name__ == "__main__":
    app.run(main)
