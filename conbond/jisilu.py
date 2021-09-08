import pandas as pd
import requests
from datetime import datetime, date
import json
import pathlib
import execjs

HEADERS = {
    'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36'
}


def auth(username, password):
    key = '397151C04723421F'
    ctx = execjs.compile(resources.read_text(__package__, 'jisilu.js'))
    s = requests.Session()
    s.post('https://www.jisilu.cn/account/ajax/login_process/',
           data={
               '_post_type': 'ajax',
               'aes': 1,
               'net_auto_login': '1',
               'password': ctx.call('jslencode', password, key),
               'return_url': 'https://www.jisilu.cn/',
               'user_name': ctx.call('jslencode', username, key),
           },
           headers=HEADERS)
    return s


def fetch(txn_day, cache_dir=None, username=None, password=None):
    jisilu_data = None
    cache_path = None

    if cache_dir:
        cache_path = pathlib.Path(cache_dir).joinpath(
            'jisilu', txn_day.strftime('%Y-%m-%d'), 'jisilu.json')
        cache_path.parent.mkdir(parents=True, exist_ok=True)

    if cache_path.exists():
        print('Using cached file: %s' % cache_path)
        jisilu_data = json.loads(cache_path.open('r').read())
    else:
        jsl = auth(username, password)
        url = 'https://www.jisilu.cn/data/cbnew/cb_list/?___jsl=LST___t=%s' % int(
            datetime.fromordinal(date.today().toordinal()).timestamp() * 1000)
        payload = {'listed': 'Y'}
        response = jsl.post(url, data=payload, headers=HEADERS)
        jisilu_data = json.loads(response.content.decode('utf-8'))

        if cache_path:
            cache_path.open('w').write(json.dumps(jisilu_data))

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
    return df[[
        'code', 'short_name', 'company_code', 'bond_price', 'stock_price',
        'convert_premium_rate', 'double_low'
    ]]
