package main.scala

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import Aggregators.{BidAskSpreadAggregator, ExtractColumnAggregator, LiquidityImbalanceAggregator, LogReturnPriceAggregator, MidPointAggregator, ReturnPriceAggregator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{abs, avg, col, expr, lit, mean, stddev_samp, sum, udf}

object SparkOrderBooks {

  def main(args: Array[String]): Unit = {

    val pair = args(0)
    val secondlyTrades = args(1)
    val s3Bucket = if(args.length == 3) args(2) else ""
    val minutes = 1 // Time series granularity
    val currentTime = LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("uuuu-MM-dd__HH-mm-ss"))

    val spark = SparkSession
      .builder()
      .appName("Spark Trades")
      .getOrCreate()
    Utilities.RegisterUtilities(spark)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val dfOrderBook = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/provider-data/order-book/unpacked/${pair}/*/*_${pair}_ob_10_*.gz")

    val dfSecondlyTrades = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/secondly-arbitrage/${secondlyTrades}")

    /* Trade Data */

    // remove outliers of price
    val dfDirtyData = dfSecondlyTrades
      .selectExpr(s"TimeIntervalFromTsToEpochMs(unix_timestamp(start_second), ${minutes * 60}) as start_ts",
        "symbol",
        "total_number_of_trades",
        "avg_price",
        "unix_timestamp(start_second) as second",
        "total_volume",
        "arbitrage_spread",
        "arbitrage_profit",
        "min_price_trades_ask",
        "max_price_trades_bid",
        "total_order_flow")
      .withColumn("arbitrage_spread_as_midpoint_bips", Utilities.ArbitrageSpreadAsMidpointPercentage)
      .withColumn("price_parity", col("avg_price") - lit(1.0))
      .where(!col("avg_price").isNaN)
      .where(Utilities.EthPaxAnomalousFilter)
      .where(Utilities.TusdUsdAnomalousFilter)

    val dfDirtyDataStats = dfDirtyData
      .groupBy("start_ts")
      .agg(
        mean("avg_price").as("mean_avg_price"),
        stddev_samp("avg_price").as("stddev_avg_price")
      )
      .withColumn("lower_bound_price", col("mean_avg_price") - lit(3)*col("stddev_avg_price"))
      .withColumn("upper_bound_price", col("mean_avg_price") + lit(3)*col("stddev_avg_price") )
      .drop("mean_avg_price", "stddev_avg_price")

    val dfTradeData = dfDirtyData
      .join(dfDirtyDataStats, "start_ts")
      .where(col("lower_bound_price") <= col("avg_price") && col("avg_price") <=  col("upper_bound_price"))
      .groupBy("start_ts", "symbol")
      .agg(
        new ReturnPriceAggregator("avg_price", "second").toColumn.as("return"),
        new LogReturnPriceAggregator("avg_price", "second").toColumn.as("logreturn"),
        sum("total_number_of_trades").cast("Double").as("total_number_of_trades"),
        sum("total_volume").cast("Double").as("total_volume"),
        avg("avg_price").as("avg_price"),
        avg("price_parity").as("price_parity"),
        avg("arbitrage_spread").as("avg_arbitrage_spread"),
        avg("arbitrage_profit").as("avg_arbitrage_profit"),
        avg("arbitrage_spread_as_midpoint_bips").as("avg_arbitrage_spread_as_midpoint_bips"),
        sum("total_order_flow").as("total_order_flow"),
      )
      .withColumn("avg_trade_size", col("total_volume")/col("total_number_of_trades"))
      //.show()

    /* Order Book Data */

    dfOrderBook.printSchema()
    dfSecondlyTrades.printSchema()

    dfOrderBook
      .withColumn("start_ts_ob", expr(s"TimeIntervalToEpochMs(date, ${minutes * 60})"))
      .join(dfTradeData, col("start_ts") === col("start_ts_ob"))
      .drop("start_ts")
      .groupBy("start_ts_ob", "symbol")
      .agg(
        new BidAskSpreadAggregator("type", "price", "amount", "avg_trade_size", "date").toColumn.as("bid_ask_spread"),
        new MidPointAggregator("type", "price", "amount", "avg_trade_size", "date").toColumn.as("midpoint_ob"),
        new LiquidityImbalanceAggregator("type", "price", "amount", "avg_trade_size", 1.0, "date").toColumn.as("liquidity_imbalance_1avg"),
        new LiquidityImbalanceAggregator("type", "price", "amount", "avg_trade_size", 2.0, "date").toColumn.as("liquidity_imbalance_2avg"),
        new LiquidityImbalanceAggregator("type", "price", "amount", "avg_trade_size", 3.0, "date").toColumn.as("liquidity_imbalance_3avg"),
        new ExtractColumnAggregator("return").toColumn.as("return"),
        new ExtractColumnAggregator("logreturn").toColumn.as("logreturn"),
        new ExtractColumnAggregator("total_number_of_trades").toColumn.as("total_number_of_trades"),
        new ExtractColumnAggregator("total_volume").toColumn.as("total_volume"),
        new ExtractColumnAggregator("avg_price").toColumn.as("avg_price"),
        new ExtractColumnAggregator("price_parity").toColumn.as("price_parity"),
        new ExtractColumnAggregator("avg_trade_size").toColumn.as("avg_trade_size"),
        new ExtractColumnAggregator("avg_arbitrage_spread").toColumn.as("avg_arbitrage_spread"),
        new ExtractColumnAggregator("avg_arbitrage_spread").toColumn.as("avg_arbitrage_profit"),
        new ExtractColumnAggregator("avg_arbitrage_spread_as_midpoint_bips").toColumn.as("avg_arbitrage_spread_as_midpoint_bips"),
        new ExtractColumnAggregator("total_order_flow").toColumn.as("total_order_flow"),
      )
      .where(col("bid_ask_spread").isNotNull)
      .withColumn("effective_spread_ob", lit(2.0) * abs(col("avg_price") - col("midpoint_ob")))
      .withColumn("date_display", expr("DisplayEpochMs(start_ts_ob)"))
      .orderBy("start_ts_ob")
//      .dropDuplicates("start_ts_ob")
//      .orderBy("date")
      .write
      .option("compression", "gzip")
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/time-series/${currentTime}_order_book_and_trades_${pair}_${minutes}m.csv.gz")

    spark.stop()
  }

}
