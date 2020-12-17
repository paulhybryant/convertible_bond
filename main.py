#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Dependencies: requests, absl-py, lxml
import json
import requests
import csv
import re
from lxml import etree
from absl import app, flags, logging
from datetime import datetime, date
from time import time

FLAGS = flags.FLAGS

flags.DEFINE_bool('debug', False, 'Debug mode')
flags.DEFINE_string('save_json', None,
                    'Whether to save the json data to file.')
flags.DEFINE_string('cc', None, '持仓信息csv文件，格式和产生的标的文件一样.')
flags.DEFINE_string('blacklist', None, '黑名单文件，格式和产生的标的文件一样.')
flags.DEFINE_integer('top', 20, 'Number of candidates')


# 获取持仓
def get_cc():
    cc_dict = {}
    with open(FLAGS.cc, 'r', encoding='utf-8') as cc_file:
        cc_reader = csv.DictReader(cc_file, delimiter=',')
        for row in cc_reader:
            if row['操作'] in ['建仓', '持仓']:
                cc_dict[row['代 码']] = row
    return cc_dict


# 获取最新转债数据
def get_dat(t):
    if FLAGS.debug:
        if not FLAGS.save_json:
            logging.fatal(
                'Need to specify name of the json file with --save_json')
        # 获取测试转债数据
        jf = open(FLAGS.save_json, 'r', encoding='utf-8')
        return json.loads(jf.read())
    else:
        # 排除未上市的
        payload = {'listed': 'Y'}
        newUrl = 'https://www.jisilu.cn/data/cbnew/cb_list/?___jsl=LST___t=%s' % int(
            t * 1000)
        logging.info(newUrl)
        # 最简单的爬虫请求.也可以加上headers字段，防止部分网址的反爬虫机制
        #  headers = {
        #  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
        #  }
        response = requests.post(newUrl, data=payload)
        # 当爬取的界面需要用户名密码登录时候，构建的请求需要包含auth字段
        data = response.content.decode('utf-8')
        if FLAGS.save_json:
            jf = open(FLAGS.save_json, 'w', encoding='utf-8')
            jf.write(data)
            jf.close()
        return json.loads(data)


def filter_reason(force_redeem, pb, btype, qflag, year_left):
    if force_redeem:
        return '公布强赎'
    if float(pb) < 1.0:
        return '破净'
    if btype != 'C':
        return '非可转债'
    if qflag == 'Q':
        return '机构可买'
    if float(year_left) < 1:
        return '剩余年限短：%s' % year_left
    return 'Bug'


# 生成转债标的
def process(dat):
    blacklist = []
    if FLAGS.blacklist:
        with open(FLAGS.blacklist, 'r', encoding='utf-8') as bl:
            bl_reader = csv.DictReader(bl, delimiter=',')
            for row in bl_reader:
                blacklist.append(row['代 码'])
    # 所有数据
    lst_data = {}
    for one in dat['rows']:
        # 每一条数据
        lst_dat = []
        # 转债id
        id = one['id']

        if id in blacklist:
            continue

        dat_cell = one['cell']
        # 是否公布强制赎回
        force_redeem = dat_cell['force_redeem']
        # 市净率
        pb = dat_cell['pb']
        # 仅机构可买
        qflag = dat_cell['qflag']
        # 债券类型，'C'为可转债，‘E'为可交换债
        btype = dat_cell['btype']
        # 剩余时间
        year_left = dat_cell['year_left']
        # 转债名称
        name = dat_cell['bond_nm']

        # 排除已经公布强赎，破净的，仅机构可买的，可交换债
        if force_redeem or float(pb) < 1.0 or btype != 'C' or qflag == 'Q' or float(year_left) < 1:
            if FLAGS.debug:
                logging.info('过滤 %s %s: %s' % (id, name, filter_reason(force_redeem, pb, btype, qflag, year_left)))
            continue

        # 现价
        price = dat_cell['price']
        # 溢价率
        premium_rt = dat_cell['premium_rt']
        # 评级
        rating_cd = dat_cell['rating_cd']
        # 回售触发价
        #  put_convert_price = dat_cell['put_convert_price']
        # 强赎触发价
        #  force_redeem_price = dat_cell['force_redeem_price']
        # 双低
        double_low = dat_cell['dblow']

        # 获取赎回价
        #  xiangqing_url = 'https://www.jisilu.cn/data/convert_bond_detail/' + id
        #  xiangqing_response = requests.get(xiangqing_url)
        #  html = xiangqing_response.content.decode('utf-8')
        #  html = etree.HTML(html)
        #  lixi = html.xpath('.//td[@id='cpn_desc']/text()')
        #  pattern = re.compile(r'\d+\.\d+?')  # 查找数字
        #  lixi = pattern.findall(lixi[0])
        #  shuhuijia = html.xpath('.//td[@id='redeem_price']/text()')
        #  li_price = 0
        #  for li in lixi:
        #  li_price = li_price + float(li)
        #  try:
        #  jiancang = float(shuhuijia[0]) + (li_price - float(lixi[-1])) * 0.8
        #  except:
        #  jiancang = 0

        lst_dat.append(id)
        lst_dat.append(name)
        lst_dat.append(price)
        lst_dat.append(premium_rt)
        lst_dat.append(pb)
        lst_dat.append(rating_cd)
        lst_dat.append(year_left)
        lst_dat.append(double_low)
        lst_data[id] = lst_dat

    # 按双低排序
    candidates = {}
    cc_dict = get_cc()
    for c in sorted(lst_data.values(), key=lambda dat: float(dat[7]))[0:FLAGS.top]:
        if FLAGS.debug:
            logging.info('%s: %s' % (c[7], ','.join(c)))
        if c[0] not in cc_dict:
            c.append('建仓')
        else:
            c.append('持仓')
        candidates[c[0]] = c

    for id, value in cc_dict.items():
        if id not in candidates:
            if id in lst_data:
                lst_data[id].append('清仓')
                candidates[id] = lst_data[id]
            else:
                value['操作'] = '清仓(过滤)'
                candidates[id] = list(value.values())

    # 返回时按操作排序
    return sorted(candidates.values(), key=lambda candidate: candidate[8])


# 输出转债标的到csv
def write_csv(data, t):
    f = open('cb%s.csv' % date.today().strftime('%Y%m%d'),
             'w', encoding='utf-8')
    csv_writer = csv.writer(f)
    csv_writer.writerow(['代 码', '转债名称', '现 价', '溢价率', '市净率', '评级',
                         '剩余年限', '双低', '操作'])
    for dat in data:
        csv_writer.writerow(dat)
    f.close()


def main(argv):
    #  t = datetime.strptime(date.today().strftime('%d/%m/%Y'), '%d/%m/%Y').timestamp() + 1
    t = time()
    dat = get_dat(t)
    data = process(dat)
    write_csv(data, t)


if __name__ == '__main__':
    app.run(main)
    flags.mark_flag_as_required('cc')
