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
    initialSecond = datetime(date.year, date.month, date.day, date.hour, residualSecondsWithinHour // 60,
                             residualSecondsWithinMinute, 0, tzinfo=timezone.utc)
    return initialSecond.timestamp()


def get_df(df, symbol, interval_minutes):
    return df \
        .select("logreturn", "total_volume", "start_ts_ob", "bid_ask_spread", "date_display", "symbol") \
        .where(col("symbol") == symbol)\
        .withColumn("ts_interval", time_interval(col("start_ts_ob"), lit(60*interval_minutes))) \
        .withColumn("volatility", col("logreturn")*col('logreturn')) \
        .groupBy("ts_interval", "symbol")\
        .agg(
            sum(col("logreturn")).alias(f"{symbol}_logreturn"),
            sum(col("total_volume")).alias(f"{symbol}_total_volume"),
            sum(col("volatility")).alias(f"{symbol}_volatility"),
            avg(col("bid_ask_spread")).alias(f"{symbol}_bid_ask_spread"))


def main(data, interval_minutes, s3_bucket):
    current_time = datetime.utcnow().strftime('%Y-%m-%d__%H-%M-%S')

    spark = SparkSession.builder.appName("SparkCrossPairCorrelation").getOrCreate()

    df = spark.read \
        .option("header", True) \
        .option("inferschema", True) \
        .csv(f"{s3_bucket}data/{data}/*_clean_order_book_and_trades_*.gz/*.gz")

    df.printSchema()

    symbols = df.select('symbol').distinct().toPandas()['symbol'].values

    schema = StructType([
        StructField("measure", StringType(), False),
        StructField("first", StringType(), False),
        StructField("second", StringType(), False),
        StructField("correlation", FloatType(), False),
        StructField("significance", FloatType(), False),
    ])
    main_df = spark.createDataFrame([], schema)

    unique_pairs = list(combinations(symbols, 2))

    for pair in unique_pairs:
        print(f'Calculating correlation for {pair}')
        first = pair[0]
        second = pair[1]

        df_first = get_df(df, first, interval_minutes).alias('df_first')
        df_second = get_df(df, second, interval_minutes).alias('df_second')

        df_join = df_first.join(df_second, df_first.ts_interval == df_second.ts_interval)
        df_pandas = df_join\
            .drop(col('df_second.ts_interval')) \
            .drop(col('df_first.symbol')) \
            .drop(col('df_second.symbol')) \
            .toPandas()

        for measure in ["logreturn", "total_volume", "volatility", "bid_ask_spread"]:
            correlation, significance = pearsonr(df_pandas[f'{first}_{measure}'], df_pandas[f'{second}_{measure}'])
            df_temp = spark.createDataFrame([(measure, first, second, correlation.item(), significance.item())], schema)
            main_df = main_df.union(df_temp)

    main_df \
        .write \
        .option("header", "true") \
        .csv(f"{s3_bucket}data/processed-data/correlations/{current_time}_crosspair_correlations_{interval_minutes}m.csv")

    spark.stop()


if __name__ == "__main__":
    parameters = json.loads(sys.argv[1])
    main(parameters['result_directory'],
         parameters['interval_minutes'],
         parameters['bucket'] if 'bucket' in parameters else "")
