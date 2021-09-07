# convertible_bond
可转债策略及量化回测

本来选择了聚宽，结果程序都写完了，发现聚宽不支持可转债回测下单。

下一步考虑用米筐或者掘金试试。

用聚宽的数据，溢价率，双低什么的需要自己算。用集思录的数据都已经算好了。
但是要量化回测的话，只能用聚宽或者米筐的API获取数据。
集思录的数据只能用于手动轮动，生成基于运行当日数据的标的和操作。

要使用聚宽的数据，需要申请使用，并将用户名密码放在auth.json里。
要使用集思录的数据，需要将用户名和密码放在auth.json里。否则集思录无法获得完整的转债数据。
集思录现在是直接用python的requests获取，比较麻烦。也可以考虑Selenium的方案。

auth.json的格式如下：
{
  "jqdata": {
    "username": "foo",
    "password": "bar"
  },
  "jisilu": {
    "username": "foo",
    "password": "bar"
  }
}

持仓信息默认放在positions.json里面

## Usage
./main.py --cache_dir=/tmp/cache --data_source=jqdata

./main.py --cache_dir=/tmp/cache --data_soruce=jqdata --txn_day=2021-08-01

./main.py --cache_dir=/tmp/cache --data_source=jisilu

./main.py --help
