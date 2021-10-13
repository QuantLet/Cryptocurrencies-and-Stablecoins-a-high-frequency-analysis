package main.scala

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import Aggregators.{ColumnOfMaxDateAggregator, ColumnOfMinDateAggregator, ExchangeOfMaxPriceBidAggregator, ExchangeOfMinPriceAskAggregator, OrderFlowAggregator, OrderFlowOfMaxPriceBidAggregator, OrderFlowOfMinPriceAskAggregator, PriceAskAggregator, PriceBidAggregator, PriceTotalAggregator, ReturnAggregator, VolumeAskAggregator, VolumeBidAggregator, VolumeOfMaxPriceBidAggregator, VolumeOfMinPriceAskAggregator, VolumeTotalAggregator}
import org.apache.spark.sql.functions.{avg, col, count, max, min, sum, udf, when}
import org.apache.spark.sql.{Encoders, SparkSession, functions}

object SparkTrades {
  def main(args: Array[String]): Unit = {

    val pair = args(0)
    val from = Utilities.ToUnixTimestampMs(args(1))
    val to = Utilities.ToUnixTimestampMs(args(2))
    val intervalPeriod = args(3).toInt // seconds
    val s3Bucket = if(args.length == 5) args(4) else ""
    val currentTime = LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("uuuu-MM-dd__HH-mm-ss"))

    val spark = SparkSession
      .builder()
      .appName("Spark Trades")
      .getOrCreate()
    spark.udf.register("TimeInterval", udf(Utilities.TimeInterval))

    val df = spark.read
      .option("header", true)
      .schema(Encoders.product[TradeRow].schema)
      .csv(s"${s3Bucket}data/provider-data/trades/*/${pair}/*/*_${pair}_trades_*.gz")

    df.printSchema()

    df
      .filter(df("date") >= from && df("date") <= to)
      .selectExpr(s"TimeInterval(date, ${intervalPeriod}) as start_second", "exchange", "symbol", "date", "price", "amount", "sell")
      .groupBy("start_second", "exchange", "symbol")
      .agg(
        count("*").as("number_of_trades"),
        PriceTotalAggregator.toColumn.as("price"),
        VolumeTotalAggregator.toColumn.as("amount"),
        OrderFlowAggregator.toColumn.name("order_flow"),
        PriceBidAggregator.toColumn.name("price_trades_bid"),
        PriceAskAggregator.toColumn.name("price_trades_ask"),
        VolumeBidAggregator.toColumn.name("volume_trades_bid"),
        VolumeAskAggregator.toColumn.name("volume_trades_ask"),
        //new ColumnOfMinDateAggregator("price").toColumn.name("price_open"),
        //new ColumnOfMaxDateAggregator("price").toColumn.name("price_close"),
      )
      .groupBy("start_second", "symbol")
      .agg(
        sum("number_of_trades").as("total_number_of_trades"),
        PriceTotalAggregator.toColumn.as("avg_price"),
        VolumeTotalAggregator.toColumn.as("total_volume"),
        min("price_trades_ask").as("min_price_trades_ask"),
        ExchangeOfMinPriceAskAggregator.toColumn.as("exchange_of_min_price_trades_ask"),
        OrderFlowOfMinPriceAskAggregator.toColumn.as("order_flow_of_min_price_trades_ask"),
        VolumeOfMinPriceAskAggregator.toColumn.as("volume_of_min_price_trades_ask"),
        max("price_trades_bid").as("max_price_trades_bid"),
        ExchangeOfMaxPriceBidAggregator.toColumn.as("exchange_of_max_price_trades_bid"),
        OrderFlowOfMaxPriceBidAggregator.toColumn.as("order_flow_of_max_price_trades_bid"),
        VolumeOfMaxPriceBidAggregator.toColumn.as("volume_of_max_price_trades_bid"),
        sum("order_flow").as("total_order_flow")
        //new ReturnAggregator("price_open", "price_close").toColumn.as("return"),
      )
      //.where(Utilities.PricesNotNaN && Utilities.ExchangesAreDifferent)
      .withColumn("arbitrage_spread", Utilities.ArbitrageSpread)
      .withColumn("arbitrage_profit", Utilities.ArbitrageProfit)
      .orderBy("start_second")
      .write
      .option("compression", "gzip")
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/secondly-arbitrage/${currentTime}_arbitrage_trades_${pair}_${Utilities.ToDate(from)}_${Utilities.ToDate(to)}_${intervalPeriod}s.csv.gz")

    spark.stop()
  }
}
