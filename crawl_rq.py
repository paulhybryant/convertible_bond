#!/usr/bin/env python3

import json
import pathlib
from absl import app, flags, logging
from datetime import date
from conbond import ricequant
import pandas as pd

FLAGS = flags.FLAGS

flags.DEFINE_string("cache_dir", None, "Cache directory")
flags.DEFINE_string("start_date", None, "Date to start")
flags.DEFINE_string("end_date", None, "Date to end")


def main(argv):
    username = None
    password = None

    auth_file = pathlib.Path('auth.json')
    auth = json.loads(auth_file.open('r').read())
    username = auth['rqdata']['username']
    password = auth['rqdata']['password']

    df_trading_dates = pd.read_excel('trading_dates.xlsx')
    if FLAGS.start_date:
        df_trading_dates = df_trading_dates[
            df_trading_dates.trading_date.dt.date >= date.fromisoformat(
                FLAGS.start_date)]
    if FLAGS.end_date:
        df_trading_dates = df_trading_dates[
            df_trading_dates.trading_date.dt.date <= date.fromisoformat(
                FLAGS.end_date)]

    ricequant.auth(username, password)
    for df_date in df_trading_dates.trading_date.tolist():
        logging.info(df_date.strftime('%Y-%m-%d'))
        ricequant.fetch(df_date, FLAGS.cache_dir, skip_process=True)


if __name__ == "__main__":
    app.run(main)
