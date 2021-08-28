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
flags.DEFINE_string('pb', None, 'pb低于1也能下调转股价的转债')
flags.DEFINE_integer('top', 20, 'Number of candidates')

flags.DEFINE_string('id', '', 'ID')


HEADER_FIELD_DICT = {
    '代 码': 'id',
    '转债名称': 'name',
    '现 价': 'price',
    '溢价率': 'premium_rt',
    '市净率': 'pb',
    '评级': 'rating_cd',
    '剩余年限': 'year_left',
    '双低': 'double_low',
    '操作': 'op',
    '建仓价': 'buy_price',
    '盈亏': 'gain'
}
BLACKLIST = []
PB = []


class ConvertibleBond():
    def set_fields(self, id, name, price, premium_rt, pb, rating_cd, year_left, double_low, force_redeem, btype, qflag):
        self.id = id
        self.name = name
        self.price = price
        self.premium_rt = premium_rt
        self.pb = pb
        self.rating_cd = rating_cd
        self.year_left = year_left
        self.double_low = double_low
        self.force_redeem = force_redeem
        self.btype = btype
        self.qflag = qflag
        self.op = None
        self.buy_price = None
        self.gain = None

    def set_fields_dict(self, dat):
        for header, attr in HEADER_FIELD_DICT.items():
            setattr(self, attr, dat[header])

    def contents(self):
        content = []
        for header, attr in HEADER_FIELD_DICT.items():
            content.append(getattr(self, attr))
        return content

# 获取持仓


def get_cc():
    cc_dict = {}
    with open(FLAGS.cc, 'r', encoding='utf-8') as cc_file:
        cc_reader = csv.DictReader(cc_file, delimiter=',')
        for row in cc_reader:
            if row['操作'] in ['建仓', '持仓']:
                cb = ConvertibleBond()
                cb.set_fields_dict(row)
                cc_dict[cb.id] = cb
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


# 排除已经公布强赎，破净的，仅机构可买的，可交换债
def filter_cb(cb):
    reason = ''
    result = True
    if cb.id in BLACKLIST:
        result = False
        reason = '黑名单'
    if cb.force_redeem:
        result = False
        reason = '公布强赎'
    if float(cb.pb) < 1.0 and cb.id not in PB:
        result = False
        reason = '破净'
    if cb.btype != 'C':
        result = False
        reason = '非可转债'
    if cb.qflag == 'Q':
        result = False
        reason = '仅机构可买'
    if float(cb.year_left) < 1:
        result = False
        reason = '剩余年限短：%s' % cb.year_left

    if FLAGS.debug and not result:
        logging.info('过滤 %s %s: %s' % (cb.id, cb.name, reason))
    return result


# 生成转债标的
def process(dat):
    if FLAGS.blacklist:
        with open(FLAGS.blacklist, 'r', encoding='utf-8') as bl:
            bl_reader = csv.DictReader(bl, delimiter=',')
            for row in bl_reader:
                BLACKLIST.append(row['代 码'])
    if FLAGS.pb:
        with open(FLAGS.pb, 'r', encoding='utf-8') as pb:
            for line in pb:
                PB.append(line.strip())
    # 所有数据
    lst_data = {}
    for one in dat['rows']:
        # 转债id
        id = one['id']

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

        cb = ConvertibleBond()
        cb.set_fields(id, name, price, premium_rt, pb, rating_cd,
                      year_left, double_low, force_redeem, btype, qflag)
        lst_data[id] = cb

    if FLAGS.id:
        logging.info('%s' % ','.join(lst_data[FLAGS.id]))
        exit()

    # 按双低排序
    candidates = {}
    cc_dict = get_cc()

    for c in sorted(filter(filter_cb, lst_data.values()), key=lambda cb: float(cb.double_low))[0:FLAGS.top]:
        if c.id not in cc_dict:
            c.op = '建仓'
            c.buy_price = c.price
            c.gain = '0%'
        else:
            c.op = '持仓'
            c.buy_price = cc_dict[c.id].price
            diff_price = round(
                (float(c.price) - float(c.buy_price)) / float(c.buy_price) * 100, 1)
            c.gain = '%s%%' % diff_price
        candidates[c.id] = c

    for id, value in cc_dict.items():
        if id not in candidates:
            diff_price = round((float(lst_data[id].price) - float(
                cc_dict[id].buy_price)) / float(cc_dict[id].buy_price) * 100, 1)
            lst_data[id].op = '清仓'
            lst_data[id].buy_price = cc_dict[id].buy_price
            lst_data[id].gain = '%s%%' % diff_price
            candidates[id] = lst_data[id]

    # 返回时按操作排序
    return sorted(candidates.values(), key=lambda candidate: candidate.op)


# 输出转债标的到csv
def write_csv(data, t):
    f = open('cb%s.csv' % date.today().strftime('%Y%m%d'),
             'w', encoding='utf-8')
    csv_writer = csv.writer(f)
    csv_writer.writerow(HEADER_FIELD_DICT.keys())
    for dat in data:
        csv_writer.writerow(dat.contents())
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
