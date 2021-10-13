import os
import sys

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def pretty(measure):
    pretty_name = {
        'avg_total_volume_in_usd_as_daily_percentage': 'Volume',
        'total_volume_in_usd': 'Volume NN',
        'avg_total_number_of_trades_as_daily_percentage': 'Number of Trades',
        'total_number_of_trades': 'Trades',
        'avg_arbitrage_profit_in_usd_as_daily_percentage': 'Profits',
        'arbitrage_profit_in_usd': 'Profits NN',
        'avg_variance_logreturns_as_daily_percentage': 'Variance',
        'variance_logreturns': 'Variance NN',
        'avg_max_arbitrage_spread_as_midpoint_bips': 'Max Arbitrage Spread',
        'avg_min_price_parity': 'Min Price Parity',
        'avg_max_price_parity': 'Max Price Parity',
        'avg_arbitrage_spread_as_midpoint_bips': 'Arbitrage Spread',
        'avg_arbitrage_spread': 'Arbitrage Spread',
        'avg_price_parity': 'Price Parity',
        'bid_ask_spread': 'Bid-Ask Spread',
        'bid_ask_spread_as_bps': 'Bid-Ask Spread',
        'effective_spread_ob': 'Effective Spread',
        'effective_spread_as_bps': 'Effective Spread',
        'total_order_flow': 'Order Flow',
        'total_order_flow_in_usd': 'Order Flow',
        'total_volume': 'Volume',
        'logreturn': 'Logreturn',
        'volatility': 'Volatility',
        'avg_arbitrage_profit': 'Arbitrage Profits',
    }
    return pretty_name[measure]


def replace_outliers_with_mean(values):
    mean = np.mean(values)
    std = np.std(values)
    return [value if np.abs(value - mean) < 3*std else mean for value in values]


def generate_single_db_plot(pair, hours, values, measure, x_ticks_frequency, sampling_frequency):
    fig, ax = plt.subplots()

    if pair in stablecoin_usd_pairs() and measure == 'avg_price_parity':
        ax.set_ylim([-0.0007, 0.017])
        values = replace_outliers_with_mean(values)

    ax.plot(hours[::sampling_frequency], values[::sampling_frequency], label=pair)
    ax.grid()
    ax.legend()
    ax.set_title(f"Hourly Breakdown of {pretty(measure)}")
    ax.set_xlabel('hours (UTC)')
    ax.set_xticks(hours[::x_ticks_frequency])

    containing_dir = f"results/figures/daily-breakdown/{pair}"
    os.makedirs(containing_dir, exist_ok=True)
    fig.savefig(f"{containing_dir}/daily-breakdown--{measure}.png", dpi=300)


def generate_daily_breakdown_plots(df, pair, x_ticks_frequency, sampling_frequency=1):
    df_pair = df[df['symbol'] == pair]
    df_sorted = df_pair.sort_values(by='start_minute_of_day')
    daily_measures = df_sorted.columns.tolist()
    daily_measures.remove('start_minute_of_day')
    daily_measures.remove('symbol')
    hours = df_sorted['start_minute_of_day'].values / 60.0
    for measure in daily_measures:
        generate_single_db_plot(pair, hours, df_sorted[measure].values, measure, x_ticks_frequency, sampling_frequency)


def main(file5m, file60m, file1m):
    df_1m = pd.read_csv(f"{file1m}")
    df_5m = pd.read_csv(f"{file5m}")
    df_60m = pd.read_csv(f"{file60m}")
    #pairs = sorted(df_1m['symbol'].unique())
    for pair in main_5_pairs():
        generate_daily_breakdown_plots(df_1m, pair, 2)
        generate_daily_breakdown_plots(df_5m, pair, 2)
        generate_daily_breakdown_plots(df_60m, pair, 2)


def stablecoin_usd_pairs():
    return ['daiusd', 'paxusd', 'tusdusd', 'usdcusd', 'usdtusd']

def main_5_pairs():
    return ['btcusd', 'btcusdt', 'ethusd', 'ethusdt', 'ethbtc', 'usdtusd']


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
