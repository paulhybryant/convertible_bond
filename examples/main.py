#!/usr/bin/env python3

import json
import pathlib
from absl import app, flags, logging
from datetime import date
from conbond import jisilu, strategy, joinquant, ricequant
import pandas as pd

FLAGS = flags.FLAGS

flags.DEFINE_string("cache_dir", None, "Cache directory")
flags.DEFINE_integer("top", 20, "Number of candidates")
flags.DEFINE_string("data_source", "jisilu",
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

    auth_file = pathlib.Path('auth.json')
    if not auth_file.exists():
        logging.fatal('auth.json is missing, see README.md')
    auth = json.load(auth_file.open('r'))
    assert FLAGS.data_source in auth
    assert 'username' in auth[FLAGS.data_source]
    assert 'password' in auth[FLAGS.data_source]
    username = auth[FLAGS.data_source]['username']
    password = auth[FLAGS.data_source]['password']

    df_trade_dates = pd.read_excel('trading_dates.xlsx')
    df_date = df_trade_dates.loc[df_trade_dates.index[
        df_trade_dates.trading_date.dt.date < date.fromisoformat(
            FLAGS.txn_day)][-1]].trading_date

    if FLAGS.data_source == 'jqdata':
        joinquant.auth(username, password)
        df = joinquant.fetch(df_date, FLAGS.cache_dir)
    elif FLAGS.data_source == 'jisilu':
        df = jisilu.fetch(df_date, FLAGS.cache_dir, username, password)
    elif FLAGS.data_source == 'rqdata':
        ricequant.auth(username, password)
        all_instruments, conversion_price, bond_price, stock_price, call_info, indicators, suspended = ricequant.fetch(
            df_date, FLAGS.cache_dir, logging)
        all_instruments = strategy.rq_filter_conbond(df_date, all_instruments,
                                                     call_info, suspended)
        df = strategy.rq_calculate_convert_premium_rate(
            all_instruments, conversion_price, bond_price, stock_price,
            indicators)

    positions_file = pathlib.Path(FLAGS.positions)
    if positions_file.exists():
        positions = json.load(positions_file.open('r'))
    else:
        positions = json.loads(
            '{"current": "NONE", "NONE": {"positions": [], "orders": {}}}')

    logging.info('Using double_low strategy')
    df_candidates = strategy.double_low(
        df, {
            'weight_bond_price': 0.5,
            'weight_convert_premium_rate': 0.5,
            'top': FLAGS.top,
        })
    candidates = set(df_candidates.index.values.tolist())
    holdings = set(positions[positions['current']]['positions'])
    orders = {}
    orders['buy'] = list(candidates - holdings)
    orders['sell'] = list(holdings - candidates)
    orders['hold'] = list(holdings & candidates)
    for k, v in orders.items():
        logging.info('%s: %s' % (k, v))
    confirm = input('Update positions (y/n)? ')
    if confirm == 'y':
        logging.info('Updating positions')
        positions['current'] = FLAGS.txn_day
        positions[positions['current']] = {}
        positions[positions['current']]['positions'] = list(candidates)
        positions[positions['current']]['orders'] = orders
        positions_file.open('w').write(json.dumps(positions))
    else:
        logging.info('Positions is not updated.')


if __name__ == "__main__":
    app.run(main)
