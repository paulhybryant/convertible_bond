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
    return post_scoring(df, txn_day, True, score_col, rank_col)


def multi_factors_weighted_linear(df, txn_day, config, score_col, rank_col):
    factors = config['factors'].keys()
    weights = config['factors'].values()
    df[score_col] = (df[factors] * weights).sum(axis=1)
    return post_scoring(df, txn_day, config['asc'], score_col, rank_col)


def post_scoring(df, txn_day, ascending, score_col, rank_col):
    df = filter(txn_day, df).sort_values(score_col, ascending=ascending).reset_index()
    df[rank_col] = df.index.to_series()
    return df.set_index('order_book_id')


def filter(txn_day, all_instruments):
    def filter_conbond(bond, txn_day=txn_day):
        # Filter bonds that have small remaining size (< 100,000,000)
        # 128060, 2019-11-20, remaining_size: 105917700.0
        #  if bond.remaining_size < 100000000:
            #  return True, '规模小于一亿: %s' % bond.remaining_size

        # Filter suspended bonds
        if bond.suspended:
            return True, '停牌'

        # Filter force redeemed bonds
        if bond.info_date is not np.nan and date.fromisoformat(
                bond.info_date) <= txn_day.date():
            return True, '已公告强赎: %s' % bond.info_date

        # Filter bonds close to maturity (30 days)
        if (date.fromisoformat(bond.maturity_date) - txn_day.date()).days < 30:
            return True, '临近赎回日: %s' % bond.maturity_date

        # Filter conbond has small volume
        if bond.volume is np.nan or bond.volume < 100 or bond.total_turnover is np.nan or bond.total_turnover < 500000:
            return True, '无/低成交, 量:%s, 额:%s' % (bond.volume,
                                                bond.total_turnover)

        return False, ''

    all_instruments[['filtered', 'filtered_reason'
                     ]] = all_instruments.apply(filter_conbond,
                                                axis=1,
                                                result_type='expand')
    return all_instruments
