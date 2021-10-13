import argparse
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import pearsonr


def filter_time_series(df, rule_freq, leader, other):
    # sampling to populate missing time points (1Min) or aggregate if larger than 1Min
    df_local = df.resample(rule=rule_freq) \
        .agg(
        {
            "start_ts_ob": "first",
            leader: np.sum,
            other: np.sum,
        })

    return df_local


def normalize(df, columns):
    df_columns = df[columns]
    df.loc[:, columns] = (df_columns - df_columns.mean()) / df_columns.std()
    return df


def linear_regression_analysis(df_y, df_x):
    regressor_names = df_x.columns.tolist() if hasattr(df_x, 'columns') else [df_x.name]
    name = f"{df_y.name} ~ {'+'.join(regressor_names) if hasattr(df_x, 'columns') else df_x.name}"
    print(name)
    y = df_y.to_numpy()
    x = df_x.to_numpy()
    model = sm.OLS(y, x)
    results = model.fit()
    return [item for item in zip(results.params, results.tvalues, results.pvalues, regressor_names)]


def calculate_correlations(df_y, df_x):
    regressor_names = df_x.columns.tolist()
    results = []
    for regressor in regressor_names:
        linear_coeffs = linear_regression_analysis(df_y, df_x[regressor])
        results.append(linear_coeffs[0])
    return results


def calculate_coefficients(df_original, lags, leader, other):
    df, ordered_regressor_columns = prepare_regression_df(df_original, lags, leader, other)
    #coefficients = linear_regression_analysis(df[f'{other}_t'], df[ordered_regressor_columns])
    coefficients = calculate_correlations(df[f'{other}_t'], df[ordered_regressor_columns])
    return [(coefficient[0], coefficient[2]) for coefficient in coefficients], [coefficient[3] for coefficient in coefficients]


def lag_suffix(shift_from_t):
    if shift_from_t == 0:
        return ""

    return f'{"+" if shift_from_t > 0 else "-"}{abs(shift_from_t)}'


def prepare_regression_df(df_original, lags, leader, other):
    assert len(lags) >= 2
    assert len(lags) % 2 == 0

    df = df_original.copy(deep=True)

    middle_lag = len(lags) // 2
    lagged_column = f'{other}_t'
    df[lagged_column] = df[other].shift(periods=middle_lag)
    df = df.drop(columns=[other])

    ordered_regressor_columns = []
    for lag in lags:
        shift_from_t = -lag + middle_lag
        lagged_column = f'{leader}_t{lag_suffix(shift_from_t)}'
        ordered_regressor_columns.append(lagged_column)
        df[lagged_column] = df[leader].shift(periods=lag)

    # removing points having missing values
    df = df.dropna()
    # rename columns
    last_regressor_column = f'{leader}_t{lag_suffix(middle_lag)}'
    df = df.rename(columns={
        leader: last_regressor_column
    })
    ordered_regressor_columns = [last_regressor_column] + ordered_regressor_columns

    return df, ordered_regressor_columns[::-1]


def combine_series(dfl, dfo):
    columns = ['start_ts_ob', 'logreturn']
    dfl_symbol = dfl['symbol'].unique()[0]
    dfo_symbol = dfo['symbol'].unique()[0]
    dfl = dfl.loc[:, columns]

    # modify leader from ethbtc to btceth
    print(f'flipping {dfl_symbol}')
    dfl['logreturn'] = -dfl['logreturn']

    dfo = dfo.loc[:, columns]
    dfl = dfl.rename(columns={'logreturn': dfl_symbol})
    dfo = dfo.rename(columns={'logreturn': dfo_symbol})
    df = dfl.merge(dfo, on='start_ts_ob', how='inner')
    return df


def main(file, files):
    df_leader = pd.read_csv(f"{file}")
    leader = df_leader['symbol'].unique()[0]

    table = []
    for f in files:
        df_other = pd.read_csv(f"{f}")
        other = df_other['symbol'].unique()[0]
        df = combine_series(df_leader, df_other)
        df = normalize(df, [leader, other])
        df['start_ts_ob_datetime'] = df['start_ts_ob'].apply(lambda x: datetime.fromtimestamp(x / 1000, tz=timezone.utc))
        df = df.sort_values(by='start_ts_ob')
        df = df.set_index(keys='start_ts_ob_datetime')

        for interval in ['5min', '1H']:
            df_clean = filter_time_series(df, interval, leader, other)
            lags = [i for i in range(1, 11)]
            coefficients, coefficient_names = calculate_coefficients(df_clean, lags, leader, other)
            table.append([other, interval] + coefficients)

    df_table = pd.DataFrame(data=table, columns=['symbol', 'interval'] + coefficient_names)
    print(df_table)
    #df_table.to_latex(f'results/tables/lead-lag-{leader}.tex', index=False, na_rep="-", float_format='%.4f')
    transform_lagged_columns(df_table, coefficient_names, lambda x: x[0]).to_csv(f'results/tables/lead-lag/lead-lag-{leader}-coefficients.csv', index=False, na_rep="-", float_format='%.4f')
    transform_lagged_columns(df_table, coefficient_names, lambda x: x[1]).to_csv(f'results/tables/lead-lag/lead-lag-{leader}-pvalue.csv', index=False, na_rep="-", float_format='%.4f')


def transform_lagged_columns(df_table, lag_columns, transformation):
    df = df_table.copy(deep=True)
    for column in lag_columns:
        df[column] = df[column].apply(transformation)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('files', nargs='+')

    args = parser.parse_args()
    all_files = args.files
    for file in all_files:
        others = [series for series in all_files if series != file]
        main(file, others)
