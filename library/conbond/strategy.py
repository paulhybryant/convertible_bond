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
        factors = config['factors'].keys()
        weights = config['factors'].values()
        result = df.copy()
        result['__rank__'] = (df[factors] * weights).sum(axis=1)
        return result.nsmallest(top, '__rank__')
