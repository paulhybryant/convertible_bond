import pandas as pd
import importlib.resources as pkg_resources

def previous_trade_date(today):
    with pkg_resources.path('conbond', 'trade_days.xlsx') as f:
        df_trade_days = pd.read_excel(f)
    return df_trade_days.loc[df_trade_days.index[df_trade_days.trade_date.dt.date < today][-1]].trade_date

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
