#!/usr/bin/env python3

import json
import pathlib
from absl import app, flags, logging
from datetime import date
from conbond import ricequant

FLAGS = flags.FLAGS

flags.DEFINE_string("cache_dir", None, "Cache directory")
flags.DEFINE_string("start_date", None, "Date to start")
flags.DEFINE_string("end_date", None, "Date to end")


def main(argv):
    username = None
    password = None

    auth_file = pathlib.Path('auth.json')
    username = auth['rqdata']['username']
    password = auth['rqdata']['password']

    df_date, df = ricequant.fetch(df_date, FLAGS.cache_dir, username,
                                      password, True)

if __name__ == "__main__":
    app.run(main)
