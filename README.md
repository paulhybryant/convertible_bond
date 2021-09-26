# convertible_bond
可转债策略及量化回测

几个平台的粗略比较：

* 聚宽
  * 转债数据行情从2018-09-13开始
  * 强赎信息好像没有
  * 用聚宽的数据，溢价率，双低什么的需要自己算
  * 可以试用半年，试用期数据量没有什么限制
  * 不支撑转债回测
  * 编程API友好，文档也不错
* 米筐
  * 数据种类最全，强赎回售什么的都有
  * 只能试用一个月，而且每天只有50MB
  * 平台试用和sdk试用分开申请，在平台上跑好像流量限制不大
  * 编程API友好，和聚宽几乎一样，文档也不错
* 掘金
  * 数据质量不足，缺少转债历史数据
  * 回测环境最友好，有本地IDE，编程方便
  * 可转债数据不足，无法回测
* 优矿
  * 数据比较全
  * 回测可转债的支持好像不太好，需要自己计算持仓，回撤等
* 集思录
  * 双低数据什么都算好了，但是没有历史转股价数据等
  * 每一个转债的历史数据有，需要爬
  * 适用于手动轮动，生成基于运行当日数据的标的和操作。

要使用聚宽的数据，需要申请使用，并将用户名密码放在auth.json里。
要使用集思录的数据，需要将用户名和密码放在auth.json里。否则集思录无法获得完整的转债数据。
集思录现在是直接用python的requests获取，比较麻烦。也可以考虑Selenium的方案。

付费的话，直接用米筐或者rqalpha-plus，交易用vnpy或者easytrader（都没试过）
免费的话，最好的组合是用米筐的数据（或者聚宽的可能也可以），用rqalpha + 自己扩展的数据源进行回测。至少对于可转债是可以work的。

auth.json的格式如下：
{
  "rqdata": {
    "username": "license",
    "password": "xxxxxxxxxx"
  },
  "jisilu": {
    "username": "foo",
    "password": "bar"
  },
  "jqdata": {
    "username": "foo",
    "password": "bar"
  },
}

持仓信息默认放在positions.json里面

Run pip install -e . in the library/ directory before running.

## Usage

./main.py --cache_dir=/tmp/cache --data_source=rqdata

./main.py --cache_dir=/tmp/cache --data_soruce=rqdata --txn_day=2021-08-01

./main.py --cache_dir=/tmp/cache --data_source=jisilu

./main.py --help

## TODO

* 过滤接近强赎触发的转债（强赎数数数据不知道有没有），强赎公告发布会马上导致溢价的收敛。
* 有的标的有的日成交量很低，下单数量超过当日Bar的25%只会部分成交，还不知道这个如何处理
* 用其他类似的量化平台结果添加测试

## Change Log

### Old
* 过滤已经公布强赎的转债: 案例：2019-09-23 127010.XSHE (Done)
* 过滤可交换债（Done)
* Handle call_info not available on some dates (Done)
* 输出Order细节到csv，转换成掘金的格式(Done)

### 2021-09-23

* 过滤停牌转债: 2021-09-08-11-39-20/issues.txt(Done)
* 过滤Q债（只有机构或者合格投资者可以购买）(Done, 目前Q债都是EB债)

### 2021-09-26

* 将rqalpha的输出转换成掘金的格式
* 用run_rq.py来运行，方便传递参数
  * ./run_rq.py --file=multi_factors.py --start_date='2021-09-01' --end_date='2021-09-23' --cache_dir='cache'
* 给回测传递参数，选择不同策略(Make conbond library strategy a mod) (Done)
* 看能否利用起rqalpha的incremental mod (Done)

### 2021-09-27

* 过滤低规模转债: 案例：128060（中装转债），2020-02-08公告，规模低于3000万，02-13开始停止交易 (Done)
