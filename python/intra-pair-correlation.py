import json
import sys
from datetime import datetime, timezone
from itertools import combinations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, sum, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from scipy.stats import pearsonr

BUCKET_NAME = 'kaiko-delivery-qfinlab-polimi'


@udf(returnType=FloatType())
def time_interval(time, interval):
    date = datetime.fromtimestamp(time / 1000, tz=timezone.utc)
    secondsFromDayStart = date.hour * 60 * 60 + date.minute * 60 + date.second
    secondInterval = secondsFromDayStart // interval
    residualSecondsWithinHour = secondInterval * interval % 3600
    residualSecondsWithinMinute = residualSecondsWithinHour - residualSecondsWithinHour // 60 * 60
    initialSecond = datetime(date.year, date.month, date.day, date.hour, residualSecondsWithinHour // 60, residualSecondsWithinMinute, 0, tzinfo=timezone.utc)
    # # calculates for day, ignores interval
    # initialSecond = datetime(date.year, date.month, date.day, 0, 0, 0, 0, tzinfo=timezone.utc)
    return initialSecond.timestamp()


def get_df(df, interval_minutes):
    return df \
        .select("start_ts_ob", "date_display", "symbol", "total_volume", "avg_arbitrage_spread", "bid_ask_spread",
                "effective_spread_ob", "total_order_flow", "total_number_of_trades", "logreturn") \
        .withColumn("ts_interval", time_interval(col("start_ts_ob"), lit(60 * interval_minutes))) \
        .withColumn("volatility", col("logreturn") * col('logreturn')) \
        .groupBy("ts_interval", "symbol") \
        .agg(
            sum(col("logreturn")).alias("logreturn"),
            sum(col("total_volume")).alias("total_volume"),
            sum(col("volatility")).alias("volatility"),
            avg(col("bid_ask_spread")).alias("bid_ask_spread"),
            avg(col("avg_arbitrage_spread")).alias("avg_arbitrage_spread"),
            avg(col("effective_spread_ob")).alias("effective_spread_ob"),
            sum(col("total_order_flow")).alias("total_order_flow"),
            sum(col("total_number_of_trades")).alias("total_number_of_trades"),
    )


def main(data, interval_minutes, s3_bucket):
    current_time = datetime.utcnow().strftime('%Y-%m-%d__%H-%M-%S')

    spark = SparkSession.builder.appName("SparkCrossPairCorrelation").getOrCreate()

    df = spark.read \
        .option("header", True) \
        .option("inferschema", True) \
        .csv(f"{s3_bucket}data/clean-time-series/{data}/*.gz")

    df.printSchema()

    symbol = df.select('symbol').first()['symbol']
    measures = ["total_volume", "avg_arbitrage_spread", "bid_ask_spread", "effective_spread_ob", "total_order_flow",
                "total_number_of_trades", "logreturn", "volatility"]

    dfSelected = get_df(df, interval_minutes)
    df_pandas = dfSelected.toPandas()

    measure_pairs = list(combinations(measures, 2))

    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("first", StringType(), False),
        StructField("second", StringType(), False),
        StructField("correlation", FloatType(), False),
        StructField("significance", FloatType(), False),
    ])
    main_df = spark.createDataFrame([], schema)

    for pair in measure_pairs:
        print(f'Calculating correlation for {pair}')
        first = pair[0]
        second = pair[1]

        correlation, significance = pearsonr(df_pandas[first], df_pandas[second])
        df_temp = spark.createDataFrame([(symbol, first, second, float(correlation), float(significance))], schema)
        main_df = main_df.union(df_temp)

    main_df \
        .coalesce(1) \
        .write \
        .option("header", "true") \
        .csv(f"{s3_bucket}data/processed-data/correlations/{current_time}_{symbol}_single_pair_corelations_{interval_minutes}m.csv")

    spark.stop()


if __name__ == "__main__":
    parameters = json.loads(sys.argv[1])
    main(parameters['result_directory'],
         parameters['interval_minutes'],
         parameters['bucket'] if 'bucket' in parameters else "")
