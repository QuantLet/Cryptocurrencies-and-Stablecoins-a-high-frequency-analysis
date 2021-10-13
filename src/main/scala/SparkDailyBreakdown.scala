package main.scala

import Aggregators.{IfNotNullAverageAggregator, IfPositiveAverageAggregator, IfPositiveCountAggregator, LogReturnPriceAggregator, RealizedVarianceAggregator, RealizedVolatilityAggregator, ReturnPriceAggregator}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object SparkDailyBreakdown {

  def main(args: Array[String]): Unit = {

    val arbitrageData = args(0)
    val usdRateData = args(1)
    val usdRateBaseCcyData = args(2)
    val minuteSequence = args(3)
    val s3Bucket = if(args.length == 5) args(4) else ""
    val minutesInInterval = 5
    val minutesInGroupingInterval = 60
    val currentTime = Utilities.CurrentTime()

    val spark = SparkSession
      .builder()
      .appName("Spark DailyBreakdown")
      .getOrCreate()
    Utilities.RegisterUtilities(spark)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val dfArbitrageData = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/secondly-arbitrage/${arbitrageData}/*.gz")
    val pair = dfArbitrageData.first().getAs[String]("symbol")

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

    val dfMinuteSequence = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/utilities/${minuteSequence}/*.gz")

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

    val df = dfArbitrageData
      .selectExpr(
        s"TimeIntervalFromTsToEpochMs(unix_timestamp(start_second), ${minutesInInterval * 60}) as start_ts",
        "unix_timestamp(start_second) as second",
        "symbol",
        "min_price_trades_ask",
        "max_price_trades_bid",
        "arbitrage_spread",
        "total_number_of_trades",
        "avg_price",
        "start_second",
        "arbitrage_profit",
        "total_volume",
        "total_order_flow",
        "DayOf(unix_timestamp(start_second)) as day",
      )
      .withColumn("arbitrage_spread_as_midpoint_bips", Utilities.ArbitrageSpreadAsMidpointPercentage)
      .withColumn("price_parity", col("avg_price") - 1)
      .where(Utilities.EthPaxAnomalousFilter)
      .where(Utilities.TusdUsdAnomalousFilter)
      .where(!col("avg_price").isNaN)
      .alias("df")
      .join(dfUsdRateData,
        col("df.day") === col("dfUsdRateData.usd_rate_day"),
        "left")
      .drop("usd_rate_day")
      .join(dfUsdRateBaseCcyData,
        col("df.day") === col("dfUsdRateBaseCcyData.day"),
        "left")
      .drop(col("dfUsdRateBaseCcyData.day"))
      .withColumn("arbitrage_profit_in_usd", Utilities.DailyArbitrageProfitUsd("arbitrage_profit")) // assume usd_rate equal to 1 (ok because it happens only for USDC and DAI)
      .withColumn("total_volume_in_usd", Utilities.DailyVolumeInUsd("total_volume"))
      .withColumn("total_order_flow_in_usd", Utilities.DailyVolumeInUsd("total_order_flow"))
      .groupBy("start_ts", "symbol")
      .agg(
        new ReturnPriceAggregator("avg_price", "second").toColumn.as("return"),
        new LogReturnPriceAggregator("avg_price", "second").toColumn.as("logreturn"),
        sum("total_number_of_trades").as("total_number_of_trades"),
        sum("total_volume_in_usd").as("total_volume_in_usd"),
        sum("total_order_flow_in_usd").as("total_order_flow_in_usd"),
        sum("arbitrage_profit_in_usd").as("arbitrage_profit_in_usd"),
        avg("arbitrage_spread_as_midpoint_bips").as("arbitrage_spread_as_midpoint_bips"),
        avg("price_parity").as("price_parity")
      )
      .join(dfMinuteSequence,
        col("start_ts") === col("start"),
        "right")
      .drop(col("start_ts"))
      .na.fill(0, Array("return", "logreturn", "total_number_of_trades", "total_volume_in_usd", "arbitrage_profit_in_usd", "total_order_flow_in_usd"))
      .na.fill(pair, Array("symbol"))
      .withColumn("day", expr("DayOfMs(start)"))
      .as("df")

    val dfDailyQuantities = df
      .groupBy("day")
      .agg(
        new RealizedVarianceAggregator("logreturn").toColumn.as("daily_variance_logreturns"),
        sum("total_number_of_trades").as("daily_total_number_of_trades"),
        sum("total_volume_in_usd").as("daily_total_volume_in_usd"),
        sum("arbitrage_profit_in_usd").as("daily_arbitrage_profit_in_usd")
      ).as("dfDailyQuantities")

    val dfAllColumns5min = df
      .join(dfDailyQuantities,
        col("df.day") === col("dfDailyQuantities.day"),
        "left")
      .drop(col("dfDailyQuantities.day"))
      .withColumn("square_logreturns", col("logreturn") * col("logreturn"))
      .withColumn("total_volume_in_usd_as_daily_percentage", safeDivide("total_volume_in_usd", "daily_total_volume_in_usd"))
      .withColumn("total_number_of_trades_as_daily_percentage", safeDivide("total_number_of_trades", "daily_total_number_of_trades"))
      .withColumn("arbitrage_profit_in_usd_as_daily_percentage", safeDivide("arbitrage_profit_in_usd", "daily_arbitrage_profit_in_usd"))
      .withColumn("square_logreturns_as_daily_percentage", safeDivide("square_logreturns", "daily_variance_logreturns"))

    val minutesInHour = 60
    val dfAllColumns1Hour = df
      .withColumn("ts_one_hour", expr(s"TimeIntervalToEpochMs(start, ${minutesInHour*60})"))
      .groupBy("ts_one_hour", "symbol")
      .agg(
        sum("total_number_of_trades").as("total_number_of_trades"),
        sum("total_volume_in_usd").as("total_volume_in_usd"),
        sum("arbitrage_profit_in_usd").as("arbitrage_profit_in_usd"),
        sum("total_order_flow_in_usd").as("total_order_flow_in_usd"),
        avg("arbitrage_spread_as_midpoint_bips").as("arbitrage_spread_as_midpoint_bips"),
        avg("price_parity").as("price_parity"),
      )
      .withColumn("day", expr("DayOfMs(ts_one_hour)"))
      .as("dfHour")
      .join(dfDailyQuantities,
        col("dfHour.day") === col("dfDailyQuantities.day"),
        "left")
      .drop(col("dfDailyQuantities.day"))
      .withColumn("total_volume_in_usd_as_daily_percentage", safeDivide("total_volume_in_usd", "daily_total_volume_in_usd"))
      .withColumn("total_number_of_trades_as_daily_percentage", safeDivide("total_number_of_trades", "daily_total_number_of_trades"))
      .withColumn("arbitrage_profit_in_usd_as_daily_percentage", safeDivide("arbitrage_profit_in_usd", "daily_arbitrage_profit_in_usd"))

    dfAllColumns1Hour.show()

    // Daily breakdown at 5 min level for all columns except volatility
    dfAllColumns1Hour
      .withColumn("start_minute_of_day", expr(s"MinuteOfTheDayMs(ts_one_hour, ${minutesInHour})"))
      .groupBy("start_minute_of_day", "symbol")
      .agg(
        avg("total_volume_in_usd_as_daily_percentage").as("avg_total_volume_in_usd_as_daily_percentage"),
        avg("total_volume_in_usd").as("total_volume_in_usd"),
        avg("total_number_of_trades_as_daily_percentage").as("avg_total_number_of_trades_as_daily_percentage"),
        avg("total_number_of_trades").as("total_number_of_trades"),
        avg("arbitrage_profit_in_usd_as_daily_percentage").as("avg_arbitrage_profit_in_usd_as_daily_percentage"),
        avg("arbitrage_profit_in_usd").as("arbitrage_profit_in_usd"),
        avg("total_order_flow_in_usd").as("total_order_flow_in_usd"),
        //avg("variance_logreturns_as_daily_percentage").as("avg_variance_logreturns_as_daily_percentage"),
        new IfNotNullAverageAggregator("arbitrage_spread_as_midpoint_bips").toColumn.as("avg_arbitrage_spread_as_midpoint_bips"),
        new IfNotNullAverageAggregator("price_parity").toColumn.as("avg_price_parity"),
      )
      .orderBy("start_minute_of_day")
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/daily_breakdown/${currentTime}_daily_breakdown_${minutesInInterval}m_${pairFromArbitrage}.csv")

    // Daily breakdown at 60 min level for volatility
    dfAllColumns5min
      .withColumn("start_ts_group", expr(s"TimeIntervalToEpochMs(start, ${minutesInGroupingInterval * 60})"))
      .groupBy("start_ts_group", "symbol")
      .agg(
        //sum("total_volume_in_usd_as_daily_percentage").as("total_volume_in_usd_as_daily_percentage"),
        //sum("total_number_of_trades_as_daily_percentage").as("total_number_of_trades_as_daily_percentage"),
        //sum("arbitrage_profit_in_usd_as_daily_percentage").as("arbitrage_profit_in_usd_as_daily_percentage"),
        sum("square_logreturns_as_daily_percentage").as("variance_logreturns_as_daily_percentage"),
        sum("square_logreturns").as("variance_logreturns"),
        //max("arbitrage_spread_as_midpoint_bips").as("max_arbitrage_spread_as_midpoint_bips"),
        //min("price_parity").as("min_price_parity"),
        //max("price_parity").as("max_price_parity")
      )
      .withColumn("display_start_ts_group", expr("DisplayEpochMs(start_ts_group)"))
      .withColumn("start_minute_of_day", expr(s"MinuteOfTheDayMs(start_ts_group, ${minutesInGroupingInterval})"))
      .groupBy("start_minute_of_day", "symbol")
      .agg(
        //avg("total_volume_in_usd_as_daily_percentage").as("avg_total_volume_in_usd_as_daily_percentage"),
        //avg("total_number_of_trades_as_daily_percentage").as("avg_total_number_of_trades_as_daily_percentage"),
        //avg("arbitrage_profit_in_usd_as_daily_percentage").as("avg_arbitrage_profit_in_usd_as_daily_percentage"),
        avg("variance_logreturns_as_daily_percentage").as("avg_variance_logreturns_as_daily_percentage"),
        avg("variance_logreturns").as("variance_logreturns"),
        //avg("max_arbitrage_spread_as_midpoint_bips").as("avg_max_arbitrage_spread_as_midpoint_bips"),
        //avg("min_price_parity").as("avg_min_price_parity"),
        //avg("max_price_parity").as("avg_max_price_parity")
      )
      .orderBy("start_minute_of_day")
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/daily_breakdown/${currentTime}_daily_breakdown_${minutesInGroupingInterval}m_${pairFromArbitrage}.csv")
//      .show()


    spark.stop()
  }

  def safeDivide(column: String, dailyColumn: String): Column = {
    when(!col(dailyColumn).isNull && col(dailyColumn) =!= 0,
      col(column).cast("Double") / col(dailyColumn).cast("Double")
    ).otherwise(col(column)) // which is zero since daily is zero
  }

}
