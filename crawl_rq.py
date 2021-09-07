#!/usr/bin/env python3

import json
import pathlib
from absl import app, flags, logging
from datetime import date
from conbond import ricequant
from conbond import core
# import pandas as pd

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

    start_date = date.fromisoformat(FLAGS.start_date)
    end_date = date.fromisoformat(FLAGS.end_date)
    df = core.trade_dates(start_date, end_date)

    ricequant.auth(username, password)
    for df_date in df.trading_date.tolist():
        logging.info(df_date.strftime('%Y-%m-%d'))
        ricequant.fetch(df_date, FLAGS.cache_dir, process=False)

if __name__ == "__main__":
    app.run(main)
