#!/usr/bin/env python3

import pathlib
from absl import app, flags, logging
import pandas as pd
from datetime import date
from tqdm import tqdm
import time

FLAGS = flags.FLAGS

flags.DEFINE_string("cache_dir", None, "Cache directory")
flags.mark_flag_as_required('cache_dir')


def main(argv):
    data_set = []
    cache_dir = pathlib.Path(FLAGS.cache_dir)
    dirs = list(cache_dir.glob('*'))
    for i in tqdm(range(0, len(dirs))):
        data_dir = dirs[i]
        p = pathlib.Path(data_dir)
        if p.is_dir():
            data = pd.read_csv(p.joinpath('bond_price.csv'))
            data_set.append(data)
    pd.concat(data_set).to_csv(cache_dir.joinpath('combined.csv'), index=False)


if __name__ == "__main__":
    app.run(main)
