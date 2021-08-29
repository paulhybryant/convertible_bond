import numpy as np
import os
import pandas as pd
import requests
from datetime import datetime, date, timedelta
import json


# To use this locally, need to call auth() first
def fetch_jqdata(jqdata, today):
    txn_day = jqdata.get_trade_days(end_date=(today - timedelta(days=1)),
                                    count=1)[0]
    df_basic_info = jqdata.bond.run_query(
        jqdata.query(jqdata.bond.CONBOND_BASIC_INFO))
    # Filter non-conbond, e.g. exchange bond
    df_basic_info = df_basic_info[df_basic_info.bond_type_id == 703013]
    # Keep active bonds only
    df_basic_info = df_basic_info[df_basic_info.list_status_id == 301001]
    df_latest_bond_price = jqdata.bond.run_query(
        jqdata.query(jqdata.bond.CONBOND_DAILY_PRICE).filter(
            jqdata.bond.CONBOND_DAILY_PRICE.date == txn_day))
    df_latest_stock_price = jqdata.get_price(
        df_basic_info.company_code.tolist(),
        start_date=txn_day,
        end_date=txn_day,
        frequency='daily',
        panel=False)
    df_convert_price_adjust = jqdata.bond.run_query(
        jqdata.query(jqdata.bond.CONBOND_CONVERT_PRICE_ADJUST))
    return txn_day, df_basic_info, df_latest_bond_price, df_latest_stock_price, df_convert_price_adjust


def massage_jqdata(df_basic_info, df_latest_bond_price, df_latest_stock_price,
                   df_convert_price_adjust):
    # Data cleaning
    df_basic_info = df_basic_info[[
        'code',
        'short_name',
        'company_code',
    ]]
    df_latest_bond_price = df_latest_bond_price[[
        'code', 'exchange_code', 'close'
    ]].rename(columns={'close': 'bond_price'})
    df_latest_stock_price = df_latest_stock_price[[
        'code', 'close'
    ]].rename(columns={'close': 'stock_price'})
    df_convert_price_adjust = df_convert_price_adjust[[
        'code', 'new_convert_price'
    ]].groupby('code').min()
    df_convert_price_adjust = df_convert_price_adjust.rename(
        columns={'new_convert_price': 'convert_price'})

    # Join basic_info with latest_bond_price to get close price from last transaction day
    # Schema: code, short_name, company_code, bond_price
    df = df_basic_info.set_index('code').join(
        df_latest_bond_price.set_index('code')).reset_index()
    # Keep only bonds that are listed and also can be traded
    # Some bonds are still listed, but is not traded (e.g. 2021-08-26, 123029)
    df = df[df.bond_price > 0]

    # Join with convert_price_adjust to get latest convert price
    # code in convert_price_latest is str, while code in df is int64
    df['code'] = df.code.astype(str)
    # Schema: code, short_name, company_code, bond_price, convert_price
    df = df.set_index('code').join(df_convert_price_adjust)

    # Join with latest_stock_price to get latest stock price
    # Schema: code, short_name, company_code, bond_price, convert_price, stock_price
    df = df.reset_index().set_index('company_code').join(
        df_latest_stock_price.set_index('code'))

    # Calculate convert_premium_rate
    # Schema: code, short_name, company_code, bond_price, convert_price, stock_price, convert_premium_rate
    df['convert_premium_rate'] = df.bond_price / (100 / df.convert_price *
                                                  df.stock_price) - 1
    return df


# config: Expect to have two keys: weight_bond_price and weight_convert_premium_rate
# df: Expect to have a column named 'double_low', or two columns named 'bond_price' and 'convert_premium_rate'
def double_low(df, config):
    assert 'top' in config
    top = config['top']

    dl_df = None
    if 'double_low' in df.columns:
        dl_df = df
    else:
        dl_df = df
        assert 'weight_bond_price' in config
        assert 'weight_convert_premium_rate' in config
        weight_bond_price = config['weight_bond_price']
        weight_convert_premium_rate = config['weight_convert_premium_rate']
        assert 'bond_price' in dl_df.columns
        assert 'convert_premium_rate' in dl_df.columns
        dl_df[
            'double_low'] = dl_df.bond_price * weight_bond_price + dl_df.convert_premium_rate * 100 * weight_convert_premium_rate
    return dl_df.nsmallest(top, 'double_low')


def generate_orders(holdings, candidates):
    orders = {}
    orders['buy'] = candidates - holdings
    orders['sell'] = holdings - candidates
    orders['hold'] = holdings & candidates
    return orders


def generate_candidates(df, strategy, strategy_config, holdings):
    candidates = strategy(df, strategy_config)
    candidates['code'] = candidates[['code', 'exchange_code']].agg('.'.join,
                                                                   axis=1)
    orders = generate_orders(holdings, set(candidates.code.tolist()))
    return candidates, orders


def fetch_jisilu():
    today = date.today()
    url = 'https://www.jisilu.cn/data/cbnew/cb_list/?___jsl=LST___t=%s' % int(
        datetime.fromordinal(today.toordinal()).timestamp() * 1000)
    payload = {'listed': 'Y'}
    response = requests.post(url, data=payload)
    # 当爬取的界面需要用户名密码登录时候，构建的请求需要包含auth字段
    data = response.content.decode('utf-8')
    return today - timedelta(days=1), data


def massage_jisiludata(data):
    jd = {}
    for row in json.loads(data)['rows']:
        jd[row['id']] = row['cell']
    df = pd.io.json.read_json(json.dumps(jd), orient='index')
    # filter exchangeable bond
    df = df[df['btype'] == 'C']
    df = df.rename(
        columns={
            'bond_id': 'code',
            'bond_nm': 'short_name',
            'stock_id': 'company_code',
            'price': 'bond_price',
            'sprice': 'stock_price',
            'premium_rt': 'convert_premium_rate',
            'dblow': 'double_low'
        }).reset_index()
    df['exchange_code'] = df.reset_index().apply(
        lambda row: 'XSHE' if row.company_code.startswith('sz') else 'XSHG',
        axis=1)
    df['code'] = df['code'].astype(str)
    return df[[
        'code', 'short_name', 'company_code', 'exchange_code', 'bond_price',
        'stock_price', 'convert_premium_rate', 'double_low'
    ]]
