package main.scala

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.functions.{col, exp, expr, lit, mean, stddev_samp}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SparkOBStatistics {

  def main(args: Array[String]): Unit = {

    val orderBookData = args(0)
    val outlier_interval_minutes = args(1).toInt
    val runCorrelations = args(2).toBoolean
    val usdRateData = args(3)
    val s3Bucket = if(args.length == 5) args(4) else ""
    val currentTime = LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("uuuu-MM-dd__HH-mm-ss"))

    val spark = SparkSession
      .builder()
      .appName("Spark OB Statistics")
      .getOrCreate()
    Utilities.RegisterUtilities(spark)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    import spark.implicits._

    val dfUsdRateData = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/daily-prices/${usdRateData}/*.gz")
      .selectExpr("day as usd_rate_day", "symbol as usd_rate", "average_daily_price as usd_rate_average_daily_price")
      .alias("dfUsdRateData")

    val dfOrderBookMeasuresDirty = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv(s"${s3Bucket}data/processed-data/time-series/${orderBookData}")
      .where(col("symbol") =!= lit("tusdusdt") || col("bid_ask_spread") < lit(1.0)) // anomalous values for tusdusdt order books
      .where(col("symbol") =!= lit("paxusd") || col("bid_ask_spread") < lit(1.0)) // anomalous values for paxusd order books
      .where(col("symbol") =!= lit("tusdusd") || col("bid_ask_spread") < lit(1.0)) // anomalous values for tusdusd order books
      .where(col("symbol") =!= lit("btcdai") || col("bid_ask_spread") < lit(1000.0)) // anomalous values
      .where(col("symbol") =!= lit("btcpax") || col("bid_ask_spread") < lit(1000.0)) // anomalous values
      .where(col("symbol") =!= lit("btctusd") || col("bid_ask_spread") < lit(1000.0)) // anomalous values
      .where(col("symbol") =!= lit("btcusdc") || col("bid_ask_spread") < lit(1000.0)) // anomalous values
      .where(col("symbol") =!= lit("ethpax") || col("bid_ask_spread") < lit(1000.0)) // anomalous values
      .where(col("symbol") =!= lit("ethtusd") || col("bid_ask_spread") < lit(1000.0)) // anomalous values
      .withColumn("outlier_group_ts", expr(s"TimeIntervalToEpochMs(start_ts_ob, ${outlier_interval_minutes * 60})"))

    val pair = dfOrderBookMeasuresDirty.first().getAs[String]("symbol")

    // filter outliers
    val dfDirtyDataStats = dfOrderBookMeasuresDirty
      .groupBy("outlier_group_ts")
      .agg(
        mean("bid_ask_spread").as("mean_bid_ask_spread"),
        stddev_samp("bid_ask_spread").as("stddev_bid_ask_spread")
      )
      .withColumn("lower_bound", col("mean_bid_ask_spread") - lit(3)*col("stddev_bid_ask_spread"))
      .withColumn("upper_bound", col("mean_bid_ask_spread") + lit(3)*col("stddev_bid_ask_spread") )
      .drop("mean_bid_ask_spread", "stddev_bid_ask_spread")

    val dfOrderBookMeasures = dfOrderBookMeasuresDirty
      .join(dfDirtyDataStats, "outlier_group_ts")
      .where(col("lower_bound") <= col("bid_ask_spread") && col("bid_ask_spread") <=  col("upper_bound"))
      .withColumn("bid_ask_spread_bps", col("bid_ask_spread") / col("midpoint_ob")*lit(10000))
      .withColumn("day", expr("DayOfMs(start_ts_ob)"))
      .join(dfUsdRateData,
        col("day") === col("dfUsdRateData.usd_rate_day"),
        "left")
      .drop("usd_rate_day")
      .withColumn("total_order_flow_usd", col("total_order_flow")*col("usd_rate_average_daily_price"))

    // save clean time series
    dfOrderBookMeasures
      .drop("outlier_group_ts", "upper_bound", "lower_bound")
      .orderBy("start_ts_ob")
      .write
      .option("compression", "gzip")
      .option("header", "true")
      .csv(s"${s3Bucket}data/clean-time-series/${currentTime}_clean_order_book_and_trades_${pair}_1m.csv.gz")

    Utilities.CalculateAndSaveStatistics(dfOrderBookMeasures, "bid_ask_spread", s"${s3Bucket}data/processed-data/statistics/${currentTime}_bid_ask_spread_${pair}_1m.csv")
    Utilities.CalculateAndSaveStatistics(dfOrderBookMeasures, "bid_ask_spread_bps", s"${s3Bucket}data/processed-data/statistics/${currentTime}_bid_ask_spread_bps_${pair}_1m.csv")
    Utilities.CalculateAndSaveStatistics(dfOrderBookMeasures, "effective_spread_ob", s"${s3Bucket}data/processed-data/statistics/${currentTime}_effective_spread_ob_${pair}_1m.csv")
    Utilities.CalculateAndSaveStatistics(dfOrderBookMeasures, "total_order_flow", s"${s3Bucket}data/processed-data/statistics/${currentTime}_total_order_flow_${pair}_1m.csv")
    Utilities.CalculateAndSaveStatistics(dfOrderBookMeasures, "total_order_flow_usd", s"${s3Bucket}data/processed-data/statistics/${currentTime}_total_order_flow_usd_${pair}_1m.csv")
    Utilities.CalculateAndSaveStatistics(dfOrderBookMeasures, "avg_arbitrage_spread_as_midpoint_bips", s"${s3Bucket}data/processed-data/statistics/${currentTime}_arbitrage_spread_bips_${pair}_1m.csv")

    // correlation
    if(runCorrelations){
      val vectorColumn = "statistics"
      val measures = Array("total_volume", "avg_arbitrage_spread", "bid_ask_spread", "effective_spread_ob", "total_order_flow", "total_number_of_trades", "logreturn", "volatility", "avg_arbitrage_profit")
      val assembler = new VectorAssembler().
        setInputCols(measures).
        setOutputCol(vectorColumn)

      val dfCorr = assembler.transform(dfOrderBookMeasures.withColumn("volatility", col("logreturn")*col("logreturn")))

      val Row(correlationMatrix: Matrix) = Correlation.corr(dfCorr, vectorColumn).head
      println(pair)
      println(s"Pearson correlation matrix:\n $correlationMatrix")

      val schema = StructType(
        StructField("symbol", StringType, false) ::
          StructField("first", StringType, false) ::
          StructField("second", StringType, false) ::
          StructField("correlation", DoubleType, false) :: Nil)
      var correlations = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      for(row <- measures.indices; column <- measures.indices){
        if(column > row){
          correlations = correlations.union(
            Seq((pair, measures.apply(row), measures.apply(column), correlationMatrix.apply(row, column))).toDF()
          )
        }
      }

      correlations
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(s"${s3Bucket}data/processed-data/correlations/${currentTime}_${pair}_single_pair_corelations.csv")
    }

    dfOrderBookMeasures.show()

    spark.stop()
  }
}
