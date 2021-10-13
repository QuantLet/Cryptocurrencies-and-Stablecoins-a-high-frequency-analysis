package main.scala

import Aggregators.{LogReturnPriceAggregator, RealizedVolatilityAggregator, ReturnPriceAggregator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, expr, lit, mean, stddev_samp, sum}

object SparkStatsAndMonthlyMeasuresWithZeroFilling {

  def main(args: Array[String]): Unit = {

    val arbitrageData = args(0)
    val minuteSequence = args(1)
    val minutes = args(2).toInt
    val s3Bucket = if (args.length == 4) args(3) else ""
    val currentTime = Utilities.CurrentTime()

    val spark = SparkSession
      .builder()
      .appName("Spark StatsAndMonthlyMeasuresWithZeroFilling")
      .getOrCreate()
    Utilities.RegisterUtilities(spark)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val dfArbitrageData = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/secondly-arbitrage/${arbitrageData}/*.gz")
    val pair = dfArbitrageData.first().getAs[String]("symbol")

    val dfMinuteSequence = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/utilities/${minuteSequence}/*.gz")

    val filterAnomalousValuesEthPax = Utilities.EthPaxAnomalousFilter
    val filterAnomalousValuesTusdUsd = Utilities.TusdUsdAnomalousFilter

    // remove outliers of price
    val dfDirtyData = dfArbitrageData
      .selectExpr(s"TimeIntervalFromTsToEpochMs(unix_timestamp(start_second), ${minutes * 60}) as start_ts", "symbol", "total_number_of_trades", "avg_price", "unix_timestamp(start_second) as second")
      .where(!col("avg_price").isNaN)
      .where(filterAnomalousValuesEthPax)
      .where(filterAnomalousValuesTusdUsd)

    val dfDirtyDataStats = dfDirtyData
      .groupBy("start_ts")
      .agg(
        mean("avg_price").as("mean_avg_price"),
        stddev_samp("avg_price").as("stddev_avg_price")
      )
      .withColumn("lower_bound_price", col("mean_avg_price") - lit(3)*col("stddev_avg_price"))
      .withColumn("upper_bound_price", col("mean_avg_price") + lit(3)*col("stddev_avg_price") )
      .drop("stddev_avg_price")

    val mean_price = dfDirtyDataStats.first().getAs[Double]("mean_avg_price")

    // calculation of measures
    val dfData = dfDirtyData
      .join(dfDirtyDataStats, "start_ts")
      .where(col("lower_bound_price") <= col("avg_price") && col("avg_price") <=  col("upper_bound_price"))
      .groupBy("start_ts", "symbol")
      .agg(
        new ReturnPriceAggregator("avg_price", "second").toColumn.as("return"),
        new LogReturnPriceAggregator("avg_price", "second").toColumn.as("logreturn"),
        sum("total_number_of_trades").cast("Double").as("total_number_of_trades"),
        avg("avg_price").as("avg_price")
      )
      //.withColumn("display_ts", expr("DisplayEpochMs(start_ts)"))

    // join with complete minute sequence to fill with zeros
    val dfDataFilled = dfMinuteSequence
      .join(dfData,
        col("start_ts") === col("start"),
        "left")
      .drop(col("start_ts"))
      .na.fill(0, Array("return", "logreturn", "total_number_of_trades"))
      .na.fill(pair, Array("symbol"))
      .na.fill(mean_price, Array("avg_price"))
      .orderBy("start")
      //.show()

    // stats for returns and number of trades
    Utilities.CalculateAndSaveStatistics(dfData, "avg_price", s"${s3Bucket}data/processed-data/statistics/${currentTime}_price_${pair}_${minutes}m.csv")
    Utilities.CalculateAndSaveStatistics(dfDataFilled, "return", s"${s3Bucket}data/processed-data/statistics/${currentTime}_returns_${pair}_${minutes}m.csv")
    Utilities.CalculateAndSaveStatistics(dfDataFilled, "logreturn", s"${s3Bucket}data/processed-data/statistics/${currentTime}_logreturns_${pair}_${minutes}m.csv")
    Utilities.CalculateAndSaveStatistics(dfDataFilled, "total_number_of_trades", s"${s3Bucket}data/processed-data/statistics/${currentTime}_number_trades_${pair}_${minutes}m.csv")

    // daily volatility
    val dfDailyVolatility = dfDataFilled
      .withColumn("day", expr("DayOfMs(start)"))
      .groupBy("day", "symbol")
      .agg(
        new RealizedVolatilityAggregator("logreturn").toColumn.as("daily_volatility_logreturns")
      )
      .withColumn("annualized_daily_volatility_logreturns", lit(math.sqrt(365)) * col("daily_volatility_logreturns"))
    Utilities.CalculateAndSaveStatistics(dfDailyVolatility, "annualized_daily_volatility_logreturns", s"${s3Bucket}data/processed-data/statistics/${currentTime}_volatility_${pair}_${minutes}m.csv")

    // monthly volatility breakdown
    dfDataFilled
      .withColumn("year_month", expr("YearAndMonthOfMs(start)"))
      .groupBy("year_month", "symbol")
      .agg(
        new RealizedVolatilityAggregator("logreturn").toColumn.as("monthly_volatility_logreturns")
      )
      .withColumn("annualized_monthly_volatility_logreturns", lit(math.sqrt(12)) * col("monthly_volatility_logreturns"))
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/monthly_arbitrage/${currentTime}_monthly_arbitrage_volatility_${pair}_${minutes}m.csv")

  }
}
