import numpy as np
import os
import pandas as pd
import requests
from datetime import datetime, date, timedelta
import json


def fetch_jqdata(username, password, jqdata, today, cache_dir, use_cache):
    df_basic_info = None
    df_latest_bond_price = None
    df_latest_stock_price = None
    df_convert_price_adjust = None
    txn_day = None

    if use_cache:
        assert cache_dir
        df_basic_info = pd.read_excel(
            os.path.join(cache_dir, 'basic_info.xlsx'))
        df_latest_bond_price = pd.read_excel(
            os.path.join(cache_dir, 'latest_bond_price.xlsx'))
        df_latest_stock_price = pd.read_excel(
            os.path.join(cache_dir, 'latest_stock_price.xlsx'))
        df_convert_price_adjust = pd.read_excel(
            os.path.join(cache_dir, 'convert_price_adjust.xlsx'))
        txn_day = df_latest_bond_price.date[0]
        assert txn_day < today, 'Cached data should be older than %s' % today
    else:
        jqdata.auth(username, password)
        txn_day = jqdata.get_trade_days(end_date=(today - timedelta(days=1)),
                                        count=1)[0]
        df_basic_info = jqdata.bond.run_query(
            jqdata.query(jqdata.bond.CONBOND_BASIC_INFO))
        # For some reason some company_code is nan
        df_basic_info = df_basic_info[pd.notnull(
            df_basic_info['company_code'])]
        df_latest_bond_price = jqdata.bond.run_query(
            jqdata.query(jqdata.bond.CONBOND_DAILY_PRICE).filter(
                jqdata.bond.CONBOND_DAILY_PRICE.date == txn_day))
        df_latest_stock_price = jqdata.get_price(
            df_basic_info.company_code.tolist(),
            start_date=txn_day,
            end_date=txn_day,
            frequency='daily',
            panel=False)
        # TODO: Add filter here instead of later
        df_convert_price_adjust = jqdata.bond.run_query(
            jqdata.query(jqdata.bond.CONBOND_CONVERT_PRICE_ADJUST))

        if cache_dir:
            df_basic_info.to_excel(os.path.join(cache_dir, 'basic_info.xlsx'))
            df_latest_bond_price.to_excel(
                os.path.join(cache_dir, 'latest_bond_price.xlsx'))
            df_latest_stock_price.to_excel(
                os.path.join(cache_dir, 'latest_stock_price.xlsx'))
            df_convert_price_adjust.to_excel(
                os.path.join(cache_dir, 'convert_price_adjust.xlsx'))

    # Data cleaning
    # Filter non-conbond, e.g. exchange bond
    df_basic_info = df_basic_info[df_basic_info.bond_type_id == 703013]
    # Filter de-listed
    df_basic_info = df_basic_info[df_basic_info.list_status_id == 301001]
    df_basic_info = df_basic_info[[
        'code',
        'short_name',
        'company_code',
    ]]
    df_latest_bond_price = df_latest_bond_price[[
        'code', 'exchange_code', 'close'
    ]].rename(columns={'close': 'bond_price'})
    # Join basic_info with latest_bond_price to get close price from last transaction day
    # Schema: code, short_name, company_code, bond_price, exchange_code
    df = df_basic_info.set_index('code').join(
        df_latest_bond_price.set_index('code')).reset_index()
    # Keep only bonds that are listed and also can be traded
    # Some bonds are still listed, but is not traded (e.g. 2021-08-26, 123029)
    df = df[df.bond_price > 0]

    # TODO: Repalce this, move it earlier
    # Filter price adjust so that the convert price is the correct one at that day
    # Using the latest one is wrong as it can be sometime newer than txn_day
    df_convert_price_adjust['adjust_date'] = pd.to_datetime(
        df_convert_price_adjust['adjust_date'])
    df_convert_price_adjust = df_convert_price_adjust[
        df_convert_price_adjust['adjust_date'].dt.date <= txn_day]
    df_convert_price_adjust = df_convert_price_adjust[[
        'code', 'new_convert_price'
    ]].groupby('code').min()
    df_convert_price_adjust = df_convert_price_adjust.rename(
        columns={'new_convert_price': 'convert_price'})

    # Join with convert_price_adjust to get latest convert price
    # code in convert_price_latest is str, while code in df is int64
    df['code'] = df.code.astype(str)
    # Schema: code, short_name, company_code, bond_price, convert_price, exchange_code
    df = df.set_index('code').join(df_convert_price_adjust)

    df_latest_stock_price = df_latest_stock_price[[
        'code', 'close'
    ]].rename(columns={'close': 'stock_price'})
    # Join with latest_stock_price to get latest stock price
    # Schema: code, short_name, company_code, bond_price, convert_price, stock_price, exchange_code
    df = df.reset_index().set_index('company_code').join(
        df_latest_stock_price.set_index('code'))

    # Calculate convert_premium_rate
    # Schema: code, short_name, company_code, bond_price, convert_price, stock_price, convert_premium_rate, exchange_code
    df['convert_premium_rate'] = df.bond_price / (100 / df.convert_price *
                                                  df.stock_price) - 1

    df['code'] = df[['code', 'exchange_code']].agg('.'.join, axis=1)
    df = df.drop(columns=['exchange_code'])
    return txn_day, df


def fetch_jisilu(user_name, password, cache_dir, use_cache):
    jisilu_data = None
    txn_day = None

    if use_cache:
        assert cache_dir
        cache = open(os.path.join(cache_dir, 'jisilu.json'),
                     'r',
                     encoding='utf-8')
        jisilu_data = json.loads(cache.read())
        txn_day = date.fromisoformat(jisilu_data['date'])
    else:
        txn_day = date.today() - timedelta(days=1)
        headers = {
            'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36'
        }
        s = requests.Session()
        s.post('https://www.jisilu.cn/account/ajax/login_process/',
               data={
                   '_post_type': 'ajax',
                   'aes': 1,
                   'net_auto_login': '1',
                   'password': password,
                   'return_url': 'https://www.jisilu.cn/',
                   'user_name': user_name,
               },
               headers=headers)
        url = 'https://www.jisilu.cn/data/cbnew/cb_list/?___jsl=LST___t=%s' % int(
            datetime.fromordinal(date.today().toordinal()).timestamp() * 1000)
        payload = {'listed': 'Y'}
        response = s.post(url, data=payload, headers=headers)
        # 当爬取的界面需要用户名密码登录时候，构建的请求需要包含auth字段
        jisilu_data = json.loads(response.content.decode('utf-8'))
        jisilu_data['date'] = txn_day.strftime('%Y-%m-%d')

        if cache_dir:
            cache = open(os.path.join(cache_dir, 'jisilu.json'),
                         'w',
                         encoding='utf-8')
            cache.write(json.dumps(jisilu_data))
            cache.close()

    jd = {}
    for row in jisilu_data['rows']:
        jd[row['id']] = row['cell']
    df = pd.io.json.read_json(json.dumps(jd), orient='index')
    # 过滤可交换债，只保留可转债
    df = df[df.btype == 'C']
    # 过滤仅机构可买
    df = df[df.qflag != 'Q']
    # 过滤已公布强赎
    df = df[pd.isnull(df.force_redeem)]
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
    df['code'] = df['code'].astype(str)
    df['code'] = df.reset_index().apply(
        lambda row: row['code'] + '.XSHE'
        if row.company_code.startswith('sz') else row['code'] + '.XSHG',
        axis=1)
    df['code'] = df['code'].astype(str)
    return txn_day, df[[
        'code', 'short_name', 'company_code', 'bond_price', 'stock_price',
        'convert_premium_rate', 'double_low'
    ]]


def fetch_rqdata(username, password, rqdatac, today, cache_dir, use_cache):
    df_basic_info = None
    df_latest_bond_price = None
    df_latest_stock_price = None
    df_convert_price_adjust = None
    txn_day = None

    if use_cache:
        assert cache_dir
        df_basic_info = pd.read_excel(
            os.path.join(cache_dir, 'rq_basic_info.xlsx'))
        df_latest_bond_price = pd.read_excel(
            os.path.join(cache_dir, 'rq_latest_bond_price.xlsx'))
        df_latest_stock_price = pd.read_excel(
            os.path.join(cache_dir, 'rq_latest_stock_price.xlsx'))
        df_convert_price_adjust = pd.read_excel(
            os.path.join(cache_dir, 'rq_convert_price_adjust.xlsx'))
        txn_day = df_latest_bond_price.date[0]
        assert txn_day < today, 'Cached data should be older than %s' % today
    else:
        rqdatac.init(username, password)
        txn_day = rqdatac.get_previous_trading_date(today)
        df_basic_info = rqdatac.convertible.all_instruments(
            txn_day).reset_index()
        df_latest_bond_price = rqdatac.get_price(
            df_basic_info.order_book_id.tolist(),
            start_date=txn_day,
            end_date=txn_day,
            frequency='1d').reset_index()
        df_latest_stock_price = rqdatac.get_price(
            df_basic_info.stock_code.tolist(),
            start_date=txn_day,
            end_date=txn_day,
            frequency='1d').reset_index()
        df_convert_price_adjust = rqdatac.convertible.get_conversion_price(
            df_basic_info.order_book_id.tolist(),
            end_date=txn_day).reset_index()

        if cache_dir:
            df_basic_info.to_excel(
                os.path.join(cache_dir, 'rq_basic_info.xlsx'))
            df_latest_bond_price.to_excel(
                os.path.join(cache_dir, 'rq_latest_bond_price.xlsx'))
            df_latest_stock_price.to_excel(
                os.path.join(cache_dir, 'rq_latest_stock_price.xlsx'))
            df_convert_price_adjust.to_excel(
                os.path.join(cache_dir, 'rq_convert_price_adjust.xlsx'))

    # Data cleaning
    # Filter non-conbond, e.g. exchange bond
    df_basic_info = df_basic_info[df_basic_info.bond_type == 'cb']
    df_basic_info = df_basic_info[[
        'order_book_id',
        'symbol',
        'stock_code',
    ]]
    df_latest_bond_price = df_latest_bond_price[[
        'order_book_id', 'close'
    ]].rename(columns={'close': 'bond_price'})
    # Join basic_info with latest_bond_price to get close price from last transaction day
    # Schema: code, short_name, company_code, bond_price
    df = df_basic_info.set_index('order_book_id').join(
        df_latest_bond_price.set_index('order_book_id')).reset_index()

    df_convert_price_adjust = df_convert_price_adjust[[
        'order_book_id', 'conversion_price'
    ]].groupby('order_book_id').min()
    df_convert_price_adjust = df_convert_price_adjust.rename(
        columns={'conversion_price': 'convert_price'})

    # Schema: order_book_id, symbol, stock_code, bond_price, convert_price
    df = df.set_index('order_book_id').join(df_convert_price_adjust)

    df_latest_stock_price = df_latest_stock_price[[
        'order_book_id', 'close'
    ]].rename(columns={'close': 'stock_price'})
    # Join with latest_stock_price to get latest stock price
    # Schema: order_book_id, short_name, company_code, bond_price, convert_price, stock_price
    df = df.reset_index().set_index('stock_code').join(
        df_latest_stock_price.set_index('order_book_id'))

    # Calculate convert_premium_rate
    # Schema: order_book_id, symbol, stock_code, bond_price, convert_price, stock_price, convert_premium_rate
    df['convert_premium_rate'] = df.bond_price / (100 / df.convert_price *
                                                  df.stock_price) - 1
    df = df.rename(columns={'order_book_id': 'code', 'symbol': 'short_name'})
    return txn_day, df


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


def generate_candidates(df, strategy, strategy_config, holdings):
    candidates = strategy(df, strategy_config)
    candidate_codes = set(candidates.code.tolist())
    orders = {}
    orders['buy'] = list(candidate_codes - holdings)
    orders['sell'] = list(holdings - candidate_codes)
    orders['hold'] = list(holdings & candidate_codes)
    return candidates, orders
