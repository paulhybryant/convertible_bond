import pandas as pd
from datetime import date


def multi_factors(df, config):
    factors = config.keys()
    weights = config.values()
    df['weighted_score'] = (df[factors] * weights).sum(axis=1)
    return df
