import numpy as np
import pandas as pd
from datetime import date
import logging


def multi_factors_rank(df, txn_day, config, score_col, rank_col):
    factors = config['factors'].keys()
    df = df.reset_index()
    df[score_col] = 0
    for factor in factors:
        df = df.sort_values(factor).reset_index(drop=True)
        df['%s_rank' % factor] = df.index.to_series()
        df[score_col] += df.index.to_series()
        df.reset_index(drop=True, inplace=True)
    return post_scoring(df, txn_day, config, score_col, rank_col)


def multi_factors_weighted_linear(df, txn_day, config, score_col, rank_col):
    factors = config['factors'].keys()
    weights = config['factors'].values()
    df[score_col] = (df[factors] * weights).sum(axis=1)
    return post_scoring(df, txn_day, config, score_col, rank_col)


def post_scoring(df, txn_day, config, score_col, rank_col):
    def filter_conbond(bond, filters=config['filters'], today=txn_day):
        values = bond.to_dict()
        values['today'] = str(today)
        for reason, cond in filters.items():
            try:
                if eval(cond.format(**values)):
                    return True, reason
            except Exception as e:
                print(cond.format(**values))
                raise e
        return False, ''
    df[['filtered', 'filtered_reason']] = df.apply(filter_conbond,
                                                   axis=1,
                                                   result_type='expand')
    df = df.sort_values(score_col, ascending=config['asc']).reset_index()
    df[rank_col] = df.index.to_series()
    return df.set_index('order_book_id')


def plot_results(results, savefile=None):
    from matplotlib import rcParams, gridspec, ticker, image as mpimg, pyplot as plt
    from matplotlib.font_manager import findfont, FontProperties
    import numpy as np

    rcParams['font.family'] = 'sans-serif'
    rcParams['font.sans-serif'] = [
        u'Microsoft Yahei',
        u'Heiti SC',
        u'Heiti TC',
        u'STHeiti',
        u'WenQuanYi Zen Hei',
        u'WenQuanYi Micro Hei',
        u'文泉驿微米黑',
        u'SimHei',
    ] + rcParams['font.sans-serif']
    rcParams['axes.unicode_minus'] = False

    use_chinese_fonts = True
    font = findfont(FontProperties(family=['sans-serif']))
    if '/matplotlib/' in font:
        use_chinese_fonts = False
        logging.warn('Missing Chinese fonts. Fallback to English.')

    title = 'Conbond Strateties Comparison'
    benchmark_portfolio = None
    start_date = None
    end_date = None
    plt.style.use('ggplot')
    img_width = 16
    img_height = 10
    fig = plt.figure(title, figsize=(img_width, img_height))
    gs = gridspec.GridSpec(img_height, img_width)
    ax = plt.subplot(gs[2:img_height, :])
    ax.get_xaxis().set_minor_locator(ticker.AutoMinorLocator())
    ax.get_yaxis().set_minor_locator(ticker.AutoMinorLocator())
    ax.grid(b=True, which='minor', linewidth=.2)
    ax.grid(b=True, which='major', linewidth=1)
    table_data = {}
    table_columns = [
        'sharpe', 'max_drawdown', 'total_returns', 'annualized_returns'
    ]
    for strategy, result_dict in results.items():
        summary = result_dict['summary']

        if benchmark_portfolio is None:
            benchmark_portfolio = result_dict.get('benchmark_portfolio')
            start_date = result_dict.get('summary')['start_date']
            end_date = result_dict.get('summary')['end_date']
            index = benchmark_portfolio.index
            portfolio_value = benchmark_portfolio.unit_net_value
            xs = portfolio_value.values
            rt = benchmark_portfolio.unit_net_value.values
            ax.plot(benchmark_portfolio['unit_net_value'] - 1.0,
                    label='HS300',
                    alpha=1,
                    linewidth=2)
            table_data['HS300'] = [
                summary['benchmark_sharpe'], summary['benchmark_max_drawdown'],
                summary['benchmark_total_returns'],
                summary['benchmark_annualized_returns']
            ]
        table_data[strategy] = [summary[col] for col in table_columns]

        portfolio = result_dict['portfolio']
        ax.plot(portfolio['unit_net_value'] - 1.0,
                label=strategy,
                alpha=1,
                linewidth=2)

    # place legend
    leg = plt.legend(loc='best')
    leg.get_frame().set_alpha(0.5)

    # manipulate axis
    vals = ax.get_yticks()
    ax.set_yticklabels(['{:3.2f}%'.format(x * 100) for x in vals])

    df = pd.DataFrame.from_dict(table_data,
                                orient='index',
                                columns=table_columns).reset_index().rename(
                                    columns={'index': 'strategy'})
    df[['max_drawdown', 'total_returns', 'annualized_returns'
        ]] = df[['max_drawdown', 'total_returns',
                 'annualized_returns']].applymap('{0:.2%}'.format)
    ax2 = plt.subplot(gs[0:2, :])
    ax2.set_title(title)
    ax2.text(0, 1, 'Start Date: %s, End Date: %s' % (start_date, end_date))
    ax2.table(cellText=df.values, colLabels=df.columns, loc='center')
    ax2.axis('off')

    if savefile:
        plt.savefig(savefile, bbox_inches='tight')
