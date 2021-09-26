#!/usr/bin/env python3

from absl import app, flags, logging
import pandas as pd

FLAGS = flags.FLAGS

flags.DEFINE_string("infile", None, "Input csv file")
flags.mark_flag_as_required('infile')
flags.DEFINE_string("outfile", None, "Output csv file")
flags.mark_flag_as_required('outfile')


def main(argv):
    orders = pd.read_csv(FLAGS.infile)
    orders = orders[[
        'order_book_id', 'trading_datetime', 'side', 'position_effect',
        'last_quantity', 'last_price'
    ]].rename(
        columns={
            'last_price': 'price',
            'last_quantity': 'volume',
            'trading_datetime': 'createdAt',
            'side': 'Side'
        })
    orders['symbol'] = orders['order_book_id'].apply(
        lambda obid: 'SZSE.%s' % obid[:6]
        if obid.endswith('XSHE') else 'SHSE.%s' % obid[:6])
    orders['side'] = orders['Side'].apply(lambda side: 1
                                          if side == 'BUY' else 2)
    orders['positionEffect'] = orders['position_effect'].apply(
        lambda pe: 1 if pe == 'OPEN' else 2)
    orders = orders[[
        'symbol', 'side', 'positionEffect', 'price', 'volume', 'createdAt'
    ]]
    orders.to_csv(FLAGS.outfile, index=False)


if __name__ == "__main__":
    app.run(main)
