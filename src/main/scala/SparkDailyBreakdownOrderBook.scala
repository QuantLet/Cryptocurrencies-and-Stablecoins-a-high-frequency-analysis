package main.scala

import Aggregators.IfNotNullAverageAggregator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, expr, sum}

object SparkDailyBreakdownOrderBook {

  def main(args: Array[String]): Unit = {
    val timeSeriesData = args(0)
    val minuteSequence = args(1)
    val s3Bucket = if(args.length == 3) args(2) else ""
    val minutesInInterval = 1 // we are setting this to 1 so it matches the time series frequency
    val currentTime = Utilities.CurrentTime()

    val spark = SparkSession
      .builder()
      .appName("Spark DailyBreakdown OrderBook")
      .getOrCreate()
    Utilities.RegisterUtilities(spark)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val dfOrderBookData = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/time-series/${timeSeriesData}/*.gz")
    val pair = dfOrderBookData.first().getAs[String]("symbol")

    val dfMinuteSequence = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/utilities/${minuteSequence}/*.gz")

    val minutesInHour = 60
    val df = dfOrderBookData
      .selectExpr(
        "start_ts_ob",
        "symbol",
        "effective_spread_ob",
        "bid_ask_spread",
        "midpoint_ob"
      )
      .withColumn("bid_ask_spread_as_bps", col("bid_ask_spread")/col("midpoint_ob")*10000)
      .withColumn("effective_spread_as_bps", col("effective_spread_ob")/col("midpoint_ob")*10000)
      .join(dfMinuteSequence,
        col("start_ts_ob") === col("start"),
        "right")
      .drop(col("start_ts_ob"))
      .na.fill(pair, Array("symbol"))
      .withColumn("ts_one_hour", expr(s"TimeIntervalToEpochMs(start, ${minutesInHour*60})"))
      .groupBy("ts_one_hour", "symbol")
      .agg(
        new IfNotNullAverageAggregator("effective_spread_as_bps").toColumn.as("effective_spread_as_bps"),
        new IfNotNullAverageAggregator("bid_ask_spread_as_bps").toColumn.as("bid_ask_spread_as_bps"),
      )
      .withColumn("start_minute_of_day", expr(s"MinuteOfTheDayMs(ts_one_hour, ${minutesInInterval})"))
      .groupBy("start_minute_of_day", "symbol")
      .agg(
        new IfNotNullAverageAggregator("effective_spread_as_bps").toColumn.as("effective_spread_as_bps"),
        new IfNotNullAverageAggregator("bid_ask_spread_as_bps").toColumn.as("bid_ask_spread_as_bps"),
      )
      .orderBy("start_minute_of_day")
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/daily_breakdown/${currentTime}_daily_breakdown_${minutesInInterval}m_${pair}.csv")
  }
}
