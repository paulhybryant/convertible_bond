#!/usr/bin/env python3

from absl import app, flags, logging
from conbond import jisilu, strategy, ricequant
from datetime import date, datetime
import json
import pandas as pd
import pathlib

FLAGS = flags.FLAGS

flags.DEFINE_string('cache_dir', None, 'Cache directory')
flags.DEFINE_integer('top', 20, 'Number of candidates')
flags.DEFINE_string('data_source', 'jisilu', 'Data source: jisilu, rqdata')
flags.DEFINE_string('positions', 'positions.json', 'File to store positions')
flags.DEFINE_string('txn_day',
                    date.today().strftime('%Y-%m-%d'),
                    'Date to generate the candidates')
flags.DEFINE_string('strategy_cfg', None, 'Strategy config')


def main(argv):
    df_date = datetime.fromordinal(
        date.fromisoformat(FLAGS.txn_day).toordinal())
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

    if FLAGS.data_source == 'jisilu':
        df_trade_dates = pd.read_excel('trading_dates.xlsx')
        df_date = df_trade_dates.loc[df_trade_dates.index[
            df_trade_dates.trading_date < df_date][-1]].trading_date
        df = jisilu.fetch(df_date, FLAGS.cache_dir, username, password)
        df = df.nsmallest(FLAGS.top, 'double_low')
        logging.info(df[[
            'short_name', 'bond_price', 'convert_premium_rate', 'double_low'
        ]])
    elif FLAGS.data_source == 'rqdata':
        # ricequant.auth(username, password)
        df = ricequant.fetch(df_date,
                             cache_dir=FLAGS.cache_dir,
                             logger=logging)
        score_col = 'double_low'
        rank_col = 'rank'
        cfg = json.load(pathlib.Path(FLAGS.strategy_cfg).open())
        logging.info(cfg['comment'])
        s = getattr(strategy, cfg['scoring_fn'])
        df = s(df, df_date, cfg['config'], score_col, rank_col)
        top = df[~df.filtered].iloc[FLAGS.top].at[rank_col]
        df = df.head(top)
        logging.info('\n%s' % df[[
            'symbol', 'bond_price', 'conversion_premium', score_col, rank_col,
            'filtered', 'filtered_reason'
        ]])

    positions_file = pathlib.Path(FLAGS.positions)
    if positions_file.exists():
        positions = json.load(positions_file.open('r'))
    else:
        positions = json.loads(
            '{"current": "NONE", "NONE": {"positions": [], "orders": {}}}')

    candidates = set(df.index.values.tolist())
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


if __name__ == '__main__':
    app.run(main)
