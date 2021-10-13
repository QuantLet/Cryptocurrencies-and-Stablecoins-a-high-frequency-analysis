package main.scala

import Aggregators.PriceTotalAggregator
import org.apache.spark.sql.functions.{count, sum, udf}
import org.apache.spark.sql.{Encoders, SparkSession}

object SparkDailyAveragePrice {
  def main(args: Array[String]): Unit = {

    val pair = args(0)
    val from = Utilities.ToUnixTimestampMs(args(1))
    val to = Utilities.ToUnixTimestampMs(args(2))
    val s3Bucket = if(args.length == 4) args(3) else ""
    val currentTime = Utilities.CurrentTime()

    val spark = SparkSession
      .builder()
      .appName("Spark DailyAveragePrice")
      .getOrCreate()
    spark.udf.register("DayOfMs", udf(Utilities.DayOfMs))

    val df = spark.read
      .option("header", true)
      .schema(Encoders.product[TradeRow].schema)
      .csv(s"${s3Bucket}data/provider-data/trades/*/${pair}/*/*_${pair}_trades_*.gz")

    df.printSchema()

    val dfDailyPrice = df
      .filter(df("date") >= from && df("date") <= to)
      .selectExpr("DayOfMs(date) as day", "exchange", "symbol", "date", "price", "amount", "sell")
      .groupBy("day", "symbol")
      .agg(
        count("*").as("number_of_trades"),
        sum("amount").as("total_amount"),
        PriceTotalAggregator.toColumn.name("average_daily_price"),
      )
      .orderBy("day")

    dfDailyPrice
      .write
      .option("compression", "gzip")
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/daily-prices/${currentTime}_average_daily_price_${pair}_${Utilities.ToDate(from)}_${Utilities.ToDate(to)}.csv.gz")

    dfDailyPrice
      .repartition(1)
      .write
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/daily-prices/${currentTime}_average_daily_price_${pair}_${Utilities.ToDate(from)}_${Utilities.ToDate(to)}.csv")

    spark.stop()
  }
}
