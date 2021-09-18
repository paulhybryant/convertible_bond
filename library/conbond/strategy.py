import pandas as pd
from datetime import date


# config: Expect to have two keys: weight_bond_price and weight_convert_premium_rate
# df: Expect to have a column named 'double_low', or two columns named 'bond_price' and 'convert_premium_rate'
# index of df is the order_book_id for the bond to place orders
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
    return df.nsmallest(top, 'double_low')


# Only works with data from ricequant now
def rq_filter_conbond(txn_day, all_instruments, call_info, suspended):
    # Filter non-conbond, e.g. exchange bond
    df = all_instruments[all_instruments.bond_type == 'cb'].set_index('order_book_id')

    # Filter bonds that stopped trading by txn_day
    #  df = df.assign(
        #  stopped_trading=lambda row: row.stop_trading_date.dt.date <= txn_day)
    #  df = df[df.stopped_trading == False]

    # Filter force redeemed bonds
    if call_info is not None and 'info_date' in call_info.columns:
        # info_date
        call_info = call_info[pd.notnull(call_info.info_date)].set_index('order_book_id')
        if not call_info.empty:
            df = df.join(call_info[['info_date']])
            df['force_redeem'] = df.info_date.dt.date < txn_day
            df = df[df.force_redeem == False]

    return df[['symbol', 'stock_code']]


# Only works with data from ricequant now
def rq_calculate_convert_premium_rate(all_instruments, conversion_price,
                                      bond_price, stock_price, indicators):
    # Add stock_price column
    stock_price = stock_price[['order_book_id',
                               'close']].rename(columns={
                                   'close': 'stock_price'
                               }).set_index('order_book_id')
    df = all_instruments.reset_index().set_index('stock_code').join(
        stock_price).reset_index().set_index('order_book_id')

    # Add bond_price column
    bond_price = bond_price[['order_book_id',
                             'close']].rename(columns={
                                 'close': 'bond_price'
                             }).set_index('order_book_id')
    df = df.join(bond_price)

    # Add conversion_price column
    conversion_price = conversion_price[['order_book_id', 'conversion_price'
                                         ]].groupby('order_book_id').min()
    df = df.join(conversion_price)

    # Calculate convert_premium_rate
    df['convert_premium_rate'] = df.bond_price / (100 / df.conversion_price *
                                                  df.stock_price) - 1
    return df
