import numpy as np
import pandas as pd
from datetime import date


def double_rank(df, txn_day, config, score_col, rank_col):
    df = df.sort_values('bond_price').reset_index()
    df['bond_price_rank'] = df.index.to_series()
    df = df.sort_values('conversion_premium')
    df['conversion_premium_rank'] = df.index.to_series()
    df[score_col] = df.bond_price_rank + df.conversion_premium_rank
    return post_scoring(df, txn_day, score_col, rank_col)

def multi_factors(df, txn_day, config, score_col, rank_col):
    factors = config.keys()
    weights = config.values()
    df[score_col] = (df[factors] * weights).sum(axis=1)
    return post_scoring(df, txn_day, score_col, rank_col)


def traditional_double_low(df, txn_day, config, score_col, rank_col):
    # 传统双低：价格 + 100 * 转股溢价率
    df[score_col] = df.bond_price + 100 * df.conversion_premium
    return post_scoring(df, txn_day, score_col, rank_col)


def post_scoring(df, txn_day, score_col, rank_col):
    df = filter(txn_day, df).sort_values(score_col).reset_index()
    df[rank_col] = df.index.to_series()
    return df


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
