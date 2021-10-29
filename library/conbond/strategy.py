import numpy as np
import pandas as pd
from datetime import date
import logging


def multi_factors_rank(df, txn_day, config, score_col, rank_col):
    factors = config['factors'].keys()
    df = df.reset_index()
    df[score_col] = 0
    for factor in factors:
        df = df.sort_values(factor).reset_index(drop=True)
        df['%s_rank' % factor] = df.index.to_series()
        df[score_col] += df.index.to_series()
        df.reset_index(drop=True, inplace=True)
    return post_scoring(df, txn_day, config, score_col, rank_col)


def multi_factors_weighted_linear(df, txn_day, config, score_col, rank_col):
    factors = config['factors'].keys()
    weights = config['factors'].values()
    df[score_col] = (df[factors] * weights).sum(axis=1)
    return post_scoring(df, txn_day, config, score_col, rank_col)


def post_scoring(df, txn_day, config, score_col, rank_col):
    df = apply_filters(df, config['filters'], txn_day)
    df = df.sort_values(score_col, ascending=config['asc']).reset_index()
    df[rank_col] = df.index.to_series()
    return df.set_index('order_book_id')


def apply_filters(df, filters, today):
    def filter_conbond(bond, filters=filters, today=today):
        values = bond.to_dict()
        values['today'] = str(today)
        for reason, cond in filters.items():
            try:
                if eval(cond.format(**values)):
                    return True, reason
            except Exception as e:
                print(cond.format(**values))
                raise e
        return False, ''

    df[['filtered', 'filtered_reason']] = df.apply(filter_conbond,
                                                   axis=1,
                                                   result_type='expand')
    return df
