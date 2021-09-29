#!/usr/bin/env python3

import pathlib
from absl import app, flags, logging
from datetime import date
import pandas as pd

FLAGS = flags.FLAGS

flags.DEFINE_string("mine", None, "Candidates of mine")
flags.DEFINE_string("theirs", None, "Candidates of theirs")
flags.DEFINE_string("filtered", None, "Filtered candidates")


def main(argv):
    mine = pd.read_csv(FLAGS.mine)
    theirs = pd.read_csv(FLAGS.theirs)
    assert len(mine) == len(theirs)
    filtered = pd.read_csv(FLAGS.filtered)
    filtered['id'] = filtered.order_book_id.apply(lambda id: id[:-5])
    filtered = filtered.set_index(['date', 'id'])
    merged = pd.concat([mine, theirs], axis=1)
    for d, indices in merged.groupby(['date']).groups.items():
        df = merged.iloc[indices].copy()
        df['id'] = df.order_book_id.apply(lambda id: id[:-5])
        mine = set(df.id.tolist())
        theirs = set(df.tickerBond.astype(str).tolist())
        df = df.set_index('id')
        for id in list(theirs - mine):
            try:
                bond = filtered.loc[(d, id)]
            except:
                logging.info(d)
                logging.error('%s not filtered by not selected by mine' % id)

        for id in list(mine - theirs):
            bond = df.loc[id]
            # If selected by mine and is in top 20, but not in theirs, see why
            if bond.at['rank'] < 20:
                logging.info(d)
                logging.error('%s in top 20 but not selected by theirs' %
                              bond.at['order_book_id'])


if __name__ == "__main__":
    app.run(main)
