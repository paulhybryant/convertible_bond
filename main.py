#!/usr/bin/env python3

import json
import pathlib
from absl import app, flags, logging
from datetime import date
from conbond import jisilu, core, joinquant, ricequant

FLAGS = flags.FLAGS

flags.DEFINE_string("cache_dir", None, "Cache directory")
flags.DEFINE_integer("top", 20, "Number of candidates")
flags.DEFINE_string("data_source", "jqdata",
                    "Data source: jqdata, jisilu, rqdata")
flags.DEFINE_string("positions", "positions.json", "File to store positions")
flags.DEFINE_string("txn_day", None, "Date to generate the candidates")


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

    if FLAGS.txn_day:
        df_date = date.fromisoformat(FLAGS.txn_day)
    else:
        df_date = date.today()

    if FLAGS.data_source == 'jqdata':
        joinquant.auth(username, password)
        df_date, df = joinquant.fetch(df_date, FLAGS.cache_dir)
    elif FLAGS.data_source == 'jisilu':
        df_date, df = jisilu.fetch(df_date, FLAGS.cache_dir, username,
                                   password)
    elif FLAGS.data_source == 'rqdata':
        ricequant.auth(username, password)
        df_date, df = ricequant.fetch(df_date, FLAGS.cache_dir)
    else:
        raise

    print(df)
    positions_file = pathlib.Path(FLAGS.positions)
    if positions_file.exists():
        positions = json.load(positions_file.open('r'))
    else:
        positions = json.loads(
            '{"current": "NONE", "NONE": {"positions": [], "orders": {}}}')

    logging.info('Using double_low strategy')
    orders = core.generate_orders(
        df, core.double_low, {
            'weight_bond_price': 0.5,
            'weight_convert_premium_rate': 0.5,
            'top': FLAGS.top,
        }, set(positions[positions['current']]['positions']))
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
