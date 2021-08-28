import unittest
import conbond
from pathlib import Path


class TestConbond(unittest.TestCase):
    def setUp(self):
        df_basic_info = None  # Basic information of the bond
        df_latest_bond_price = None  # Latest price of the bond
        df_latest_stock_price = None  # Latest price of the stock
        df_convert_price_adjust = None  # Latest convert price of the bond
        df_date = None  # Date of the price information
        df_date, df_basic_info, df_latest_bond_price, df_latest_stock_price, df_convert_price_adjust = conbond.fetch_cache(
            '%s/testdata' % Path(__file__).parent)
        self.df = conbond.massage_jqdata(df_basic_info, df_latest_bond_price,
                                         df_latest_stock_price,
                                         df_convert_price_adjust)

    def test_double_low(self):
        candidates = conbond.execute_strategy(
            self.df, conbond.double_low, {
                'weight_bond_price': 0.5,
                'weight_convert_premium_rate': 0.5,
                'top': 20,
            })
        hold = set(['128100', '110080', '128037'])
        sell = set(['1', '2', '3']);
        holdings = set.union(sell, hold)
        orders = conbond.generate_orders(holdings, set(candidates.code.tolist()))
        self.assertTrue('buy' in orders)
        self.assertTrue('sell' in orders)
        self.assertTrue('hold' in orders)
        self.assertEqual(orders['buy'], set(candidates.code.tolist()) - hold)
        self.assertEqual(orders['sell'], sell)
        self.assertEqual(orders['hold'], hold)


if __name__ == '__main__':
    unittest.main()
