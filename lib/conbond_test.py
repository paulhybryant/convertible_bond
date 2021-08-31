import unittest
import conbond
from pathlib import Path


class TestConbond(unittest.TestCase):
    def setUp(self):
        df_date, self.df = conbond.fetch_jqdata(
            None, None, '%s/testdata' % Path(__file__).parent, True)

    def test_double_low(self):
        hold = set(['113033.XSHG'])
        sell = set(['1', '2', '3'])
        holdings = set.union(sell, hold)
        candidates, orders = conbond.generate_candidates(
            self.df, conbond.double_low, {
                'weight_bond_price': 0.5,
                'weight_convert_premium_rate': 0.5,
                'top': 20,
            }, holdings)
        self.assertTrue('buy' in orders)
        self.assertTrue('sell' in orders)
        self.assertTrue('hold' in orders)
        print(candidates.code.tolist())
        self.assertEqual(orders['buy'], set(candidates.code.tolist()) - hold)
        self.assertEqual(orders['sell'], sell)
        self.assertEqual(orders['hold'], hold)


if __name__ == '__main__':
    unittest.main()
