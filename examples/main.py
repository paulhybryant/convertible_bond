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
    df_date = date.fromisoformat(FLAGS.txn_day)
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

    if FLAGS.data_source == 'jqdata':
        joinquant.auth(username, password)
        df = joinquant.fetch(df_date, FLAGS.cache_dir)
    elif FLAGS.data_source == 'jisilu':
        df_trade_dates = pd.read_excel('trading_dates.xlsx')
        df_date = df_trade_dates.loc[df_trade_dates.index[
            df_trade_dates.trading_date.dt.date < date.fromisoformat(
                FLAGS.txn_day)][-1]].trading_date
        df = jisilu.fetch(df_date, FLAGS.cache_dir, username, password)
    elif FLAGS.data_source == 'rqdata':
        ricequant.auth(username, password)
        df = ricequant.fetch(df_date,
                                          cache_dir=FLAGS.cache_dir,
                                          logger=logging)
        logging.info('过滤标的：%s' % df[(df.bond_type == 'cb') & (df.filtered)][['symbol', 'filtered_reason']])
        df = df[~df.filtered]

    positions_file = pathlib.Path(FLAGS.positions)
    if positions_file.exists():
        positions = json.load(positions_file.open('r'))
    else:
        positions = json.loads(
            '{"current": "NONE", "NONE": {"positions": [], "orders": {}}}')

    logging.info('Using multi_factors strategy')
    df_candidates = strategy.multi_factors(
        df, {
            'factors': {
                'bond_price': 0.5,
                'conversion_premium': 0.5 * 100,
            },
            'top': FLAGS.top,
        })
    logging.info(df_candidates[['symbol', 'bond_price', 'conversion_premium', '__rank__']])
    candidates = set(df_candidates.index.values.tolist())
    position_date = positions['current']
    holdings = set(positions[position_date]['positions'])
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
