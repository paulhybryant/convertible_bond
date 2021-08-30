# convertible_bond
可转债策略及量化回测

本来选择了聚宽，结果程序都写完了，发现聚宽不支持可转债回测下单。

下一步考虑用米筐试试。

用聚宽的数据，溢价率，双低什么的需要自己算。用集思录的数据都已经算好了。
但是要量化回测的话，只能用聚宽或者米筐的API获取数据。
集思录的数据只能用于手动轮动，生成基于某个交易日的标的和操作。

要使用聚宽的数据，需要申请使用，并将用户名密码放在jqconfig.json里。
要使用集思录的数据，需要将用户名和密码放在jisilu.json里。否则集思录无法获得完整的转债数据。

## Usage
./main.py --nouse_cache --cache_dir=$PWD/lib/testdata --data_source=jqdata

./main.py --use_cache --cache_dir=$PWD/lib/testdata --data_soruce=jqdata

./main.py --help
