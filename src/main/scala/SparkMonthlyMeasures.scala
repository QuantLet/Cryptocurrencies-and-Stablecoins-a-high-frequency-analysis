package main.scala

import Aggregators.{IfPositiveAverageAggregator, IfPositiveCountAggregator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkMonthlyArbitrageMeasures {
  def main(args: Array[String]): Unit = {

    val arbitrageData = args(0)
    val usdRateData = args(1)
    val usdRateBaseCcyData = args(2)
    val s3Bucket = if(args.length == 4) args(3) else ""
    val currentTime = Utilities.CurrentTime()

    val spark = SparkSession
      .builder()
      .appName("Spark MonthlyArbitrageMeasures")
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

    val dfUsdRateBaseCcyData = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/daily-prices/${usdRateBaseCcyData}/*.gz")
      .selectExpr("day", "symbol as usd_rate_base_ccy", "average_daily_price as usd_rate_base_ccy_average_daily_price")
      .alias("dfUsdRateBaseCcyData")

    dfArbitrageData.printSchema()
    dfUsdRateData.printSchema()
    dfUsdRateBaseCcyData.printSchema()

    val pairFromArbitrage = dfArbitrageData.first().getAs[String]("symbol")
    val pairFromUsdRate = dfUsdRateData.first().getAs[String]("usd_rate")
    val pairFromUsdRateBaseCcy = dfUsdRateBaseCcyData.first().getAs[String]("usd_rate_base_ccy")
    assert("usd" == pairFromUsdRate.takeRight(3), s"Not a USD rate !! ${pairFromUsdRate}")
    assert("usd" == pairFromUsdRateBaseCcy.takeRight(3), s"Not a USD rate !! ${pairFromUsdRateBaseCcy}")
    assert(pairFromArbitrage.contains(pairFromUsdRate.substring(0, pairFromUsdRate.length - 3)), s"Incompatible pairs ${pairFromArbitrage} - ${pairFromUsdRate}")
    assert(pairFromArbitrage.contains(pairFromUsdRateBaseCcy.substring(0, pairFromUsdRateBaseCcy.length - 3)), s"Incompatible pairs ${pairFromArbitrage} - ${pairFromUsdRateBaseCcy}")

    val filterAnomalousValues = Utilities.EthPaxAnomalousFilter

    /* arbitrage spread related columns */
    val dfArbitrageData1s = dfArbitrageData
      .selectExpr("YearAndMonthOf(unix_timestamp(start_second)) as year_month", "symbol", "min_price_trades_ask", "max_price_trades_bid", "arbitrage_spread",
        "total_number_of_trades", "avg_price", "start_second")
      .withColumn("arbitrage_spread_as_midpoint_bips", Utilities.ArbitrageSpreadAsMidpointPercentage)
      .where(filterAnomalousValues)

    Utilities.CalculateAndSaveStatistics(dfArbitrageData1s, "avg_price", s"${s3Bucket}data/processed-data/statistics/${currentTime}_price_${pairFromArbitrage}.csv")
    //Utilities.CalculateAndSaveStatistics(dfArbitrageData1s, "total_number_of_trades", s"${s3Bucket}data/processed-data/statistics/${currentTime}_number_trades_${pairFromArbitrage}.csv")

    val dfMonthlyArbitrageSpread = dfArbitrageData1s
      .groupBy("year_month", "symbol")
      .agg(
        new IfPositiveAverageAggregator("arbitrage_spread_as_midpoint_bips").toColumn.as("average_arbitrage_spread_bips"),
        new IfPositiveCountAggregator("arbitrage_spread_as_midpoint_bips").toColumn.as("number_of_seconds_arbitrage_spread_positive"),
        //avg("arbitrage_spread_as_midpoint_bips").as("average_arbitrage_spread_bips"),
        sum("total_number_of_trades").as("total_number_of_trades"),
        avg("avg_price").as("avg_price"),
        stddev("avg_price").as("stddev_price")
      )
      .withColumn("ratio_stddev_avg_price", col("stddev_price") / col("avg_price"))
      .alias("dfMonthlyArbitrageSpread")
      //.show()

    /* arbitrage profit and volume converted to USD using average daily prices for conversion rates */
    val dfMonthlyProfitVolume1d = dfArbitrageData
      .selectExpr("DayOf(unix_timestamp(start_second)) as day", "symbol", "arbitrage_profit", "total_volume")
      .groupBy("day", "symbol")
      .agg(
        sum("arbitrage_profit").as("daily_arbitrage_profit"),
        sum("total_volume").as("daily_total_volume"),
      )
      .alias("dfDailyProfit")
      .join(dfUsdRateData,
        col("dfDailyProfit.day") === col("dfUsdRateData.usd_rate_day"),
        "left")
      .drop("usd_rate_day")
      .join(dfUsdRateBaseCcyData,
        col("dfDailyProfit.day") === col("dfUsdRateBaseCcyData.day"),
        "left")
      .drop(col("dfUsdRateBaseCcyData.day"))
      .withColumn("daily_arbitrage_profit_in_usd", Utilities.DailyArbitrageProfitUsd("daily_arbitrage_profit")) // assume usd_rate equal to 1 (ok because it happens only for USDC and DAI)
      .withColumn("daily_total_volume_in_usd", Utilities.DailyVolumeInUsd("daily_total_volume"))

    Utilities.CalculateAndSaveStatistics(dfMonthlyProfitVolume1d, "daily_total_volume_in_usd", s"${s3Bucket}data/processed-data/statistics/${currentTime}_volume_${pairFromArbitrage}.csv")

    val dfMonthlyProfit = dfMonthlyProfitVolume1d
      .withColumn("year_month", expr("YearAndMonthOfString(day)"))
      .groupBy("year_month", "symbol")
      .agg(
        sum("daily_arbitrage_profit_in_usd").as("arbitrage_profit_in_usd"),
        avg("usd_rate_average_daily_price").as("usd_rate_average_price"),
        sum("daily_total_volume_in_usd").as("total_volume_in_usd")
      ).alias("dfMonthlyProfit")
      //.show()

    /* returns at 1 minute frequency */
    val csvPathForReturnStatistics = s"${s3Bucket}data/processed-data/statistics/${currentTime}_returns_${pairFromArbitrage}.csv"
    val dfMonthlyVolatilityOfReturns = Utilities.GetMonthlyVolatilityColumns(dfArbitrageData, "avg_price", filterAnomalousValues, csvPath = csvPathForReturnStatistics) //monthly assuming every day is a trading day
      .alias("dfMonthlyVolatilityOfReturns")


    /* combine all monthly columns */
    dfMonthlyArbitrageSpread.join(dfMonthlyProfit,
      col("dfMonthlyArbitrageSpread.year_month") === col("dfMonthlyProfit.year_month"),
      "inner")
      .drop(col("dfMonthlyProfit.year_month"))
      .drop(col("dfMonthlyProfit.symbol"))
      .join(dfMonthlyVolatilityOfReturns,
        col("dfMonthlyArbitrageSpread.year_month") === col("dfMonthlyVolatilityOfReturns.year_month"),
        "inner")
      .drop(col("dfMonthlyVolatilityOfReturns.year_month"))
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/monthly_arbitrage/${currentTime}_monthly_arbitrage_measures_${pairFromArbitrage}.csv")

    spark.stop()
  }

}
