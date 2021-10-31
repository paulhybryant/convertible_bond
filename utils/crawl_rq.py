#!/usr/bin/env python3

import json
import pathlib
from absl import app, flags, logging
from datetime import date, timedelta
from conbond import ricequant
import pandas as pd
from tqdm import tqdm
import logging

FLAGS = flags.FLAGS

flags.DEFINE_string('cache_dir', None, 'Cache directory')
flags.mark_flag_as_required('cache_dir')
flags.DEFINE_string('start_date',
                    date.today().strftime('%Y-%m-%d'), 'Date to start')
flags.DEFINE_string('end_date',
                    date.today().strftime('%Y-%m-%d'), 'Date to end')


def main(argv):
    username = None
    password = None

    auth_file = pathlib.Path('.auth.json')
    auth = json.loads(auth_file.open('r').read())
    username = auth['rqdata']['username']
    password = auth['rqdata']['password']

    df_trading_dates = pd.read_excel('trading_dates.xlsx')
    df_trading_dates = df_trading_dates[
        df_trading_dates.trading_date.dt.date >= date.fromisoformat(
            FLAGS.start_date)]
    df_trading_dates = df_trading_dates[df_trading_dates.trading_date.dt.date
                                        <= date.fromisoformat(FLAGS.end_date)]

    ricequant.auth(username, password)
    dates = df_trading_dates.trading_date.to_list()
    for i in tqdm(range(0, len(dates))):
        ricequant.fetch(dates[i], FLAGS.cache_dir, logging)


if __name__ == '__main__':
    app.run(main)
