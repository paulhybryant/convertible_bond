import pandas as pd
import importlib.resources as resources


def previous_trade_date(today):
    with resources.path(__package__, 'trading_dates.xlsx') as f:
        df_trade_dates = pd.read_excel(f)
    return df_trade_dates.loc[df_trade_dates.index[
        df_trade_dates.trading_date.dt.date < today][-1]].trading_date


def trade_dates(start_date=None, end_date=None):
    with resources.path(__package__, 'trading_dates.xlsx') as f:
        df_trade_dates = pd.read_excel(f)
    if start_date:
        df_trade_dates = df_trade_dates[df_trade_dates.trading_date.dt.date >= start_date]
    if end_date:
        df_trade_dates = df_trade_dates[df_trade_dates.trading_date.dt.date <= end_date]
    return df_trade_dates
    
    
# config: Expect to have two keys: weight_bond_price and weight_convert_premium_rate
# df: Expect to have a column named 'double_low', or two columns named 'bond_price' and 'convert_premium_rate'
# index of df is the id for the bond to place order
def double_low(df, config):
    assert 'top' in config
    top = config['top']

    if 'double_low' not in df.columns:
        assert 'weight_bond_price' in config
        assert 'weight_convert_premium_rate' in config
        weight_bond_price = config['weight_bond_price']
        weight_convert_premium_rate = config['weight_convert_premium_rate']
        assert 'bond_price' in df.columns
        assert 'convert_premium_rate' in df.columns
        df['double_low'] = df.bond_price * weight_bond_price + df.convert_premium_rate * 100 * weight_convert_premium_rate
    dl = df.nsmallest(top, 'double_low')
    print(dl)
    return set(df.nsmallest(top, 'double_low').index.values.tolist())


def generate_orders(df, strategy, strategy_config, holdings):
    candidates = strategy(df, strategy_config)
    orders = {}
    orders['buy'] = list(candidates - holdings)
    orders['sell'] = list(holdings - candidates)
    orders['hold'] = list(holdings & candidates)
    return orders
