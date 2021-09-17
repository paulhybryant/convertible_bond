#!/usr/bin/env python3

import json
import pickle
import pandas as pd
import rqdatac
import pathlib
from absl import flags, app

FLAGS = flags.FLAGS

flags.DEFINE_string("infile", None, "instrument.pk")
flags.DEFINE_string("outfile", None, "instrument_with_convertible.pk")


def main(argv):
    username = None
    password = None

    data_source = 'rqdata'
    auth_file = pathlib.Path('auth.json')
    if not auth_file.exists():
        logging.fatal('auth.json is missing, see README.md')
    auth = json.load(auth_file.open('r'))
    assert 'username' in auth[data_source]
    assert 'password' in auth[data_source]
    username = auth[data_source]['username']
    password = auth[data_source]['password']

    rqdatac.init(username, password)
    conbonds = rqdatac.convertible.all_instruments()
    #  conbonds.to_excel('conbonds.xlsx', index=False)
    #  conbonds = pd.read_excel('conbonds.xlsx')
    conbonds['type'] = 'CS'
    conbonds['round_lot'] = 10
    conbonds['board_type'] = 'MainBoard'
    with open(FLAGS.infile, 'rb') as f:
        instruments = pickle.load(f)
        instruments += conbonds.to_dict('records')
        with open(FLAGS.outfile, 'wb') as out:
            pickle.dump(instruments, out, protocol=2)


if __name__ == "__main__":
    app.run(main)
