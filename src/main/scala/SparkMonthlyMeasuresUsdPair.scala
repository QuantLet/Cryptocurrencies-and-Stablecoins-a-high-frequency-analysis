package main.scala

import Aggregators.{IfPositiveAverageAggregator, IfPositiveCountAggregator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkMonthlyArbitrageMeasuresUsdPair {
  def main(args: Array[String]): Unit = {

    val arbitrageData = args(0)
    val usdRateData = args(1)
    val s3Bucket = if(args.length == 3) args(2) else ""
    val currentTime = Utilities.CurrentTime()

    val spark = SparkSession
      .builder()
      .appName("Spark MonthlyArbitrageMeasuresUsdPair")
      .getOrCreate()
    Utilities.RegisterUtilities(spark)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val dfArbitrageData = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/secondly-arbitrage/${arbitrageData}/*.gz")

    val dfUsdRateData = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/daily-prices/${usdRateData}/*.gz")
      .selectExpr("day as usd_rate_day", "symbol as usd_rate", "average_daily_price as usd_rate_average_daily_price")
      .alias("dfUsdRateData")

    dfArbitrageData.printSchema()
    dfUsdRateData.printSchema()

    val pairFromArbitrage = dfArbitrageData.first().getAs[String]("symbol")
    val pairFromUsdRate = dfUsdRateData.first().getAs[String]("usd_rate")
    assert("usd" == pairFromArbitrage.takeRight(3), s"Not a USD rate !! ${pairFromArbitrage}")
    assert("usd" == pairFromUsdRate.takeRight(3), s"Not a USD rate !! ${pairFromUsdRate}")

    val filterAnomalousValues = Utilities.TusdUsdAnomalousFilter

    /* arbitrage spread related columns */
    val dfArbitrageData1s = dfArbitrageData
      .selectExpr("YearAndMonthOf(unix_timestamp(start_second)) as year_month",
        "symbol",
        "min_price_trades_ask",
        "max_price_trades_bid",
        "arbitrage_spread",
        "arbitrage_profit",
        "total_number_of_trades",
        "avg_price"
      )
      .withColumn("arbitrage_spread_as_midpoint_bips", Utilities.ArbitrageSpreadAsMidpointPercentage)
      .where(filterAnomalousValues)
      .where(!col("avg_price").isNaN)

    Utilities.CalculateAndSaveStatistics(dfArbitrageData1s, "avg_price", s"${s3Bucket}data/processed-data/statistics/${currentTime}_price_${pairFromArbitrage}.csv")
    //Utilities.CalculateAndSaveStatistics(dfArbitrageData1s, "total_number_of_trades", s"${s3Bucket}data/processed-data/statistics/${currentTime}_number_trades_${pairFromArbitrage}.csv")

    /* arbitrage profit and volume already in USD */
    val dfMonthlyArbitrage = dfArbitrageData1s
      .groupBy("year_month", "symbol")
      .agg(
        new IfPositiveAverageAggregator("arbitrage_spread_as_midpoint_bips").toColumn.as("average_arbitrage_spread_bips"),
        new IfPositiveCountAggregator("arbitrage_spread_as_midpoint_bips").toColumn.as("number_of_seconds_arbitrage_spread_positive"),
        //avg("arbitrage_spread_as_midpoint_bips").as("average_arbitrage_spread_bips"),
        sum("total_number_of_trades").as("total_number_of_trades"),
        avg("avg_price").as("avg_price"),
        stddev("avg_price").as("stddev_price"),
        sum("arbitrage_profit").as("arbitrage_profit_in_usd")
      )
      .withColumn("ratio_stddev_avg_price", col("stddev_price") / col("avg_price"))
      .select("year_month", "symbol", "average_arbitrage_spread_bips","number_of_seconds_arbitrage_spread_positive",
      "total_number_of_trades", "avg_price", "stddev_price", "ratio_stddev_avg_price", "arbitrage_profit_in_usd")
      .alias("dfMonthlyArbitrage")
      //.show()

    val dfVolume1d = dfArbitrageData
      .selectExpr("DayOf(unix_timestamp(start_second)) as day", "symbol", "total_volume")
      .groupBy("day", "symbol")
      .agg(
        sum("total_volume").as("daily_total_volume"),
      )
      .alias("dfDailyVolume")
      .join(dfUsdRateData,
        col("dfDailyVolume.day") === col("dfUsdRateData.usd_rate_day"),
        "left")
      .drop(col("dfUsdRateData.usd_rate_day"))
      .withColumn("usd_rate_base_ccy_average_daily_price", col("usd_rate_average_daily_price"))
      .withColumn("daily_total_volume_in_usd", Utilities.DailyVolumeInUsd("daily_total_volume")) // assume usd_rate equal to 1 (ok because it happens only for USDC and DAI)

    Utilities.CalculateAndSaveStatistics(dfVolume1d, "daily_total_volume_in_usd", s"${s3Bucket}data/processed-data/statistics/${currentTime}_volume_${pairFromArbitrage}.csv")

    val dfMonthlyVolume = dfVolume1d
      .withColumn("year_month", expr("YearAndMonthOfString(day)"))
      .groupBy("year_month", "symbol")
      .agg(
        sum("daily_total_volume_in_usd").as("total_volume_in_usd")
      ).alias("dfMonthlyVolume")
      //.show()

    val dfMonthlyUsdRate = dfUsdRateData
      .selectExpr("YearAndMonthOfString(usd_rate_day) as year_month", "usd_rate", "usd_rate_average_daily_price")
      .groupBy("year_month", "usd_rate")
      .agg(
        avg("usd_rate_average_daily_price").as("usd_rate_average_price")
      )
      .alias("dfMonthlyUsdRate")
      //.show()


    /* returns at 1 minute frequency */
    val csvPathForReturnStatistics = s"${s3Bucket}data/processed-data/statistics/${currentTime}_returns_${pairFromArbitrage}.csv"
    val dfMonthlyVolatilityOfReturns = Utilities.GetMonthlyVolatilityColumns(dfArbitrageData, "avg_price", filterAnomalousValues, csvPath = csvPathForReturnStatistics) //monthly assuming every day is a trading day
      .alias("dfMonthlyVolatilityOfReturns")

    /* combine all monthly columns */
    dfMonthlyArbitrage.join(dfMonthlyUsdRate,
      col("dfMonthlyArbitrage.year_month") === col("dfMonthlyUsdRate.year_month"),
      "inner")
      .drop(col("dfMonthlyUsdRate.year_month"))
      .drop(col("dfMonthlyUsdRate.usd_rate"))
      .join(dfMonthlyVolume,
        col("dfMonthlyArbitrage.year_month") === col("dfMonthlyVolume.year_month"),
        "inner")
      .drop(col("dfMonthlyVolume.year_month"))
      .drop(col("dfMonthlyVolume.symbol"))
      .join(dfMonthlyVolatilityOfReturns,
        col("dfMonthlyArbitrage.year_month") === col("dfMonthlyVolatilityOfReturns.year_month"),
        "inner")
      .drop(col("dfMonthlyVolatilityOfReturns.year_month"))
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/monthly_arbitrage/${currentTime}_monthly_arbitrage_measures_${pairFromArbitrage}.csv")

    spark.stop()
  }

}
