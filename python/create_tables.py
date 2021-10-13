import string
import sys

import pandas as pd
import numpy as np
import argparse
from plot_daily_breakdown import pretty


def build_monthly_row(df, pair, months, column, multiplier=1.):
    row = [pair]
    for month in months:
        pair_and_month = is_pair_and_month(df, month, pair)
        values = df[pair_and_month][column].values
        assert (len(values) <= 1)
        data = values[0] * multiplier if pair_and_month.any() else None
        row.append(data)
    return row


def is_pair_and_month(df, month, pair):
    return (df['symbol'] == pair) & (df['year_month'] == month)


def build_table(df, pairs, months, column, tex_file, number_format, multiplier=1.):
    df_table = make_df(column, df, months, multiplier, pairs)
    print(column)
    print(df_table)
    df_table.to_latex(f'results/tables/{tex_file}', index=False, na_rep="-", float_format=number_format)
    return df_table


def make_df(column, df, months, multiplier, pairs):
    table = []
    for pair in pairs:
        table.append(build_monthly_row(df, pair, months, column, multiplier))
    df_table = pd.DataFrame(data=table, columns=['symbol'] + months)
    return df_table


def sum_monthly_values(df, pair, months):
    numeric_series = df[df['symbol'] == pair][months].values
    return numeric_series[~np.isnan(numeric_series)].sum()


def average_monthly_values(df, df_weights, pair, months):
    numeric_series = df[df['symbol'] == pair][months].values
    numeric_series = numeric_series[~np.isnan(numeric_series)]
    numeric_weights = df_weights[df_weights['symbol'] == pair][months].values
    numeric_weights = numeric_weights[~np.isnan(numeric_weights)]
    return np.sum(numeric_series * numeric_weights) / np.sum(numeric_weights)


def combine_arbitrage_tables(df_profits, df_arbitrage_opportunities, df_arbitrage_spread, tex_file):
    pairs = df_profits['symbol']
    months = df_profits.columns.tolist()
    months.remove('symbol')
    table = []
    table_net = []
    for pair in pairs:
        profits = sum_monthly_values(df_profits, pair, months)
        arbitrage_opportunities = sum_monthly_values(df_arbitrage_opportunities, pair, months)
        spread = average_monthly_values(df_arbitrage_spread, df_arbitrage_opportunities, pair, months)
        table.append([pair, profits, arbitrage_opportunities, spread])
        fees = 0.001
        bps = 0.0001
        table_net.append([pair, profits, spread, profits / (spread * bps) * max(0, spread * bps - 2 * fees)])
    df_table = pd.DataFrame(data=table, columns=['symbol', 'profits', 'opportunities', 'spread'])
    df_table_net = pd.DataFrame(data=table_net, columns=['symbol', 'profits', 'spread', 'net_profits'])
    df_table.to_latex(f'results/tables/{tex_file}', index=False, na_rep="-")
    df_table_net.to_latex(f'results/tables/{tex_file[:-4]}-net-profits.tex', index=False, na_rep="-")
    print(df_table)


def single_pair_tables(df, months, pairs):
    df_profits = build_table(df, pairs, months, 'arbitrage_profit_in_usd', 'arbitrage-profits.tex', '%d')
    df_arbitrage_spread = build_table(df, pairs, months, 'average_arbitrage_spread_bips',
                                      'arbitrage-spreads-average.tex', '%.2f')
    df_arbitrage_opportunities = build_table(df, pairs, months, 'number_of_seconds_arbitrage_spread_positive',
                                             'arbitrage-spreads-count.tex', '%d')
    combine_arbitrage_tables(df_profits, df_arbitrage_opportunities, df_arbitrage_spread,
                             'single-arbitrage-summary.tex')
    build_table(df, pairs, months, 'total_volume_in_usd', 'arbitrage-volume.tex', '%.2f', 1e-6)
    build_table(df, pairs, months, 'ratio_stddev_avg_price', 'arbitrage-volatility.tex', '%.2f', 100)
    build_table(df, pairs, months, 'volatility_minute_returns_monthly', 'arbitrage-return-volatility.tex', '%.2f', 100)
    build_table(df, pairs, months, 'total_number_of_trades', 'arbitrage-total-trades.tex', '%.2f', 1e-3)
    build_table(df, pairs, months, 'annualized_monthly_volatility_logreturns', 'arbitrage-logreturn-volatility.tex',
                '%.2f', 100)


def triangulation_tables(df, months, pairs):
    df_profits_sell = build_table(df, pairs, months, 'sell_arbitrage_profit_in_usd',
                                  'triangle-sell-arbitrage-profits.tex', '%d')
    df_spread_sell = build_table(df, pairs, months, 'average_sell_arbitrage_spread_bips',
                                 'triangle-sell-arbitrage-spreads-average.tex', '%.2f')
    df_opportunities_sell = build_table(df, pairs, months, 'number_of_seconds_sell_arbitrage_spread_positive',
                                        'triangle-sell-arbitrage-spreads-count.tex', '%d')
    combine_arbitrage_tables(df_profits_sell, df_opportunities_sell, df_spread_sell,
                             'triangle-sell-arbitrage-summary.tex')

    df_profits_buy = build_table(df, pairs, months, 'buy_arbitrage_profit_in_usd', 'triangle-buy-arbitrage-profits.tex',
                                 '%d')
    df_spread_buy = build_table(df, pairs, months, 'average_buy_arbitrage_spread_bips',
                                'triangle-buy-arbitrage-spreads-average.tex', '%.2f')
    df_opportunities_buy = build_table(df, pairs, months, 'number_of_seconds_buy_arbitrage_spread_positive',
                                       'triangle-buy-arbitrage-spreads-count.tex', '%d')
    combine_arbitrage_tables(df_profits_buy, df_opportunities_buy, df_spread_buy, 'triangle-buy-arbitrage-summary.tex')

    build_table(df, pairs, months, 'total_volume_in_usd', 'triangle-arbitrage-volume.tex', '%.2f', 1e-3)
    build_table(df, pairs, months, 'ratio_stddev_avg_price', 'triangle-arbitrage-volatility.tex', '%.2f', 100)
    build_table(df, pairs, months, 'volatility_minute_returns_monthly', 'triangle-arbitrage-return-volatility.tex',
                '%.2f', 100)


def arbitrage_tables(file, type_arbitrage):
    df = pd.read_csv(f"{file}")
    pairs = sorted(df['symbol'].unique())
    months = sorted(df['year_month'].unique())
    if type_arbitrage == 'single':
        single_pair_tables(df, months, pairs)
    elif type_arbitrage == 'triangle':
        triangulation_tables(df, months, pairs)
    else:
        raise Exception(f'unrecognized type arbitrage {type_arbitrage}')


def build_stats_table(df, quantity):
    df_quantity = df[df['type'] == quantity]
    if df_quantity.empty:
        return
    sorted_df = df_quantity.sort_values(by='symbol')
    columns = sorted_df.columns.tolist()
    columns.remove('type')
    columns.remove('symbol')
    if quantity != 'total_number_of_trades':
        columns.remove('percentage_of_zeros')
    final_df = sorted_df[['symbol'] + columns]
    print(final_df)
    desired_pair_order(final_df).to_latex(f'results/tables/stats_{quantity}.tex', index=False, na_rep="-")


def stats_table(file):
    df = pd.read_csv(f"{file}")
    df = df.drop_duplicates(subset=['symbol', 'type'])
    for quantity in ['avg_price', 'return', 'total_number_of_trades', 'daily_total_volume_in_usd',
                     'annualized_daily_volatility_logreturns', 'logreturn', 'bid_ask_spread', 'effective_spread_ob',
                     'total_order_flow', 'avg_arbitrage_spread_as_midpoint_bips', 'bid_ask_spread_bps', 'total_order_flow_usd']:
        build_stats_table(df, quantity)


def parity_table(file):
    df = pd.read_csv(f"{file}")
    pairs = df['symbol'].unique()
    table = []
    for pair in pairs:
        values = df[df['symbol'] == pair]['avg_price_parity']
        table.append([pair, values.min(), values.max(), values.std()])
    df_table = pd.DataFrame(data=table, columns=['symbol', 'min', 'max', 'stdev'])
    df_table.to_latex(f'results/tables/price_parity.tex', index=False, na_rep="-")
    print(df_table)


def correlation_table(file5, file60, threshold, significance_threshold):
    df_5m = pd.read_csv(f"{file5}")
    df_60m = pd.read_csv(f"{file60}")

    pairs = sorted(df_5m['first'].append(df_5m['second']).unique())
    #pairs = sorted(main_pairs())
    df_5m = df_5m[df_5m['first'].isin(pairs) & df_5m['second'].isin(pairs)]
    df_60m = df_60m[df_60m['first'].isin(pairs) & df_60m['second'].isin(pairs)]
    measures = df_5m['measure'].unique()

    for measure in measures:
        df_correlation_5m, df_significance_5m = build_correlation_matrix(df_5m, measure, pairs, threshold)
        df_correlation_60m, df_significance_60m = build_correlation_matrix(df_60m, measure, pairs, threshold)
        df_correlation = df_correlation_5m + df_correlation_60m.T
        df_significance = df_significance_5m + df_significance_60m.T
        np.fill_diagonal(df_correlation.values, 1)
        if significance_threshold:
            df_significance[df_significance >= significance_threshold] = None
        df_correlation.to_latex(f'results/tables/correlation-{measure}.tex', index=True, float_format='%.2f', na_rep="-")
        df_significance.to_latex(f'results/tables/correlation-significance-{measure}.tex', index=True, float_format='%.3f',
                                 na_rep="-")
        print(measure)
        print(df_correlation)


def build_correlation_matrix(df, measure, pairs, threshold):
    table = []
    table_significance = []
    for pair in pairs:
        df_measure = df[df['measure'] == measure]
        table.append(get_correlation_row(df_measure, pair))
        table_significance.append(get_significance_row(df_measure, pair))
    df_correlation = pd.DataFrame(data=table, columns=pairs, index=pairs)
    df_significance = pd.DataFrame(data=table_significance, columns=pairs, index=pairs)
    if threshold:
        df_correlation[df_correlation.abs() < threshold] = None
    df_correlation = lower_triangle_to_value(df_correlation, 0)
    df_significance = lower_triangle_to_value(df_significance, 0)
    return df_correlation, df_significance


def get_correlation_row(df, main_entity):
    correlations = inner_get_correlation_row(df, main_entity)
    return correlations['correlation'].values.tolist()


def get_significance_row(df, main_entity):
    correlations = inner_get_correlation_row(df, main_entity)
    return correlations['significance'].values.tolist()


def inner_get_correlation_row(df, main_entity):
    correlations = df[(df['first'] == main_entity) | (df['second'] == main_entity)]
    correlations['other_entity'] = np.where(correlations['first'] != main_entity, correlations['first'],
                                            correlations['second'])
    correlations = correlations.append(
        {'first': main_entity, 'second': main_entity, 'correlation': 1., 'other_entity': main_entity,
         'significance': 0.},
        ignore_index=True)
    correlations = correlations.sort_values(by='other_entity')
    return correlations


def single_pair_correlation_table(file5, file60, significance_threshold):
    df_5m = pd.read_csv(f"{file5}")
    df_60m = pd.read_csv(f"{file60}")

    for undesired_measure in ['avg_arbitrage_spread', 'effective_spread_ob']:
        df_5m = df_5m[(df_5m['first'] != undesired_measure) & (df_5m['second'] != undesired_measure)]
        df_60m = df_60m[(df_60m['first'] != undesired_measure) & (df_60m['second'] != undesired_measure)]

    #pairs = sorted(df_5m['symbol'].unique())
    pairs = main_pairs()
    measures = sorted(df_5m['first'].append(df_5m['second']).unique())
    measure_names = [pretty(measure) for measure in measures]
    df_correlation = pd.DataFrame(data=[], columns=['symbol', 'measure'] + measure_names)
    df_significance = pd.DataFrame(data=[], columns=['symbol', 'measure'] + measure_names)
    for pair in pairs:
        df_single_5m = df_5m[df_5m['symbol'] == pair]
        df_single_60m = df_60m[df_60m['symbol'] == pair]
        table_5m = []
        table_60m = []
        significance_5m = []
        significance_60m = []
        for measure in measures:
            table_5m.append(get_correlation_row(df_single_5m, measure))
            table_60m.append(get_correlation_row(df_single_60m, measure))
            significance_5m.append(get_significance_row(df_single_5m, measure))
            significance_60m.append(get_significance_row(df_single_60m, measure))
        table = np.tril(table_5m) + np.triu(table_60m)
        np.fill_diagonal(table, 1)
        single_pair_df = pd.DataFrame(data=table, columns=measure_names)
        single_pair_df['measure'] = measure_names
        single_pair_df['symbol'] = pair
        df_correlation = pd.concat([df_correlation, single_pair_df], ignore_index=True)

        table_significance = np.tril(significance_5m) + np.triu(significance_60m)
        np.fill_diagonal(table_significance, 0)
        single_pair_significance_df = pd.DataFrame(data=table_significance, columns=measure_names)
        if significance_threshold:
            single_pair_significance_df[single_pair_significance_df >= significance_threshold] = None
        single_pair_significance_df['measure'] = measure_names
        single_pair_significance_df['symbol'] = pair
        df_significance = pd.concat([df_significance, single_pair_significance_df], ignore_index=True)

    df_correlation.to_latex(f'results/tables/single-pair-measure-correlations.tex', index=False, float_format='%.2f',
                            na_rep="-")
    df_significance.to_latex(f'results/tables/single-pair-measure-correlations-significance.tex', index=False,
                             float_format='%.3f', na_rep="-")
    print(df_correlation)


def lower_triangle_to_value(df_correlation, value):
    rows = len(df_correlation)
    df_correlation = df_correlation.mask(np.arange(rows)[:, None] > np.arange(rows), value)
    return df_correlation


def desired_pair_order(df):
    desired_order = ['btcdai',
                     'btcpax',
                     'btctusd',
                     'btcusdc',
                     'btcusdt',
                     'ethdai',
                     'ethpax',
                     'ethtusd',
                     'ethusdc',
                     'ethusdt',
                     'ethbtc',
                     'btcusd',
                     'ethusd',
                     'daiusd',
                     'paxusd',
                     'tusdusd',
                     'usdcusd',
                     'usdtusd',
                     'paxusdt',
                     'tusddai',
                     'tusdusdt',
                     'daiusdc',
                     'usdcusdt',
                     'usdtdai']
    desired_order = ['btcusdt', 'ethusdt', 'ethbtc', 'btcusd', 'ethusd', 'usdtusd']
    desired_index_order = [pd.Index(df['symbol']).get_loc(i) for i in desired_order]
    return df.iloc[desired_index_order]


def main_pairs():
    return ['btcusd', 'ethusd', 'btcusdt', 'ethusdt', 'ethbtc', 'usdtusd']


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('file')
    parser.add_argument('--file60', dest='file60')
    parser.add_argument('--type', dest='type', default='single')
    parser.add_argument('--stats', dest='stats', action='store_true')
    parser.add_argument('--parity', dest='parity', action='store_true')
    parser.add_argument('--correlation', dest='correlation', action='store_true')
    parser.add_argument('--single_pair_corr', dest='single_pair_corr', action='store_true')
    parser.add_argument('--threshold', dest='threshold', type=float)
    parser.add_argument('--significance_threshold', dest='significance_threshold', type=float)
    parser.set_defaults(stats=False)
    parser.set_defaults(parity=False)
    parser.set_defaults(correlation=False)
    parser.set_defaults(single_pair_corr=False)

    args = parser.parse_args()
    if args.stats:
        stats_table(args.file)
    elif args.parity:
        parity_table(args.file)
    elif args.correlation:
        correlation_table(args.file, args.file60, args.threshold, args.significance_threshold)
    elif args.single_pair_corr:
        single_pair_correlation_table(args.file, args.file60, args.significance_threshold)
    else:
        arbitrage_tables(args.file, args.type)
