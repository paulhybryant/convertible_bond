import pandas as pd
from datetime import date


# index of df is the order_book_id for the bond to place orders
def multi_factors(df, config):
    assert 'top' in config
    top = config['top']

    # jisilu
    if 'double_low' in df.columns:
        return df.nsmallest(top, 'double_low')
    else:
        assert 'factors' in config
        df['__rank__'] = 0
        for factor, weight in config['factors'].items():
            assert factor in df.columns
            df['__rank__'] = df.__rank__ + df[factor] * weight
        return df.nsmallest(top, '__rank__')


# Only works with data from ricequant now
def rq_filter_conbond(txn_day, all_instruments):
    # Filter non-conbond, e.g. exchange bond
    df = all_instruments[all_instruments.bond_type == 'cb']

    # Filter bonds that have small remaining size

    # Filter force redeemed bonds
    df = df[(df.info_date < txn_day.strftime('%Y-%m-%d')).eq(False)]
    return df
