package main.scala

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SparkParityUsdRate {
  def main(args: Array[String]): Unit = {

    val fromDate = Utilities.ToDateTimeMs(args(0))
    val toDate = Utilities.ToDateTimeMs(args(1))
    val s3Bucket = if(args.length == 3) args(2) else ""
    val currentTime = Utilities.CurrentTime()

    val spark = SparkSession
      .builder()
      .appName("Spark SparkParityUsdRate")
      .getOrCreate()
    Utilities.RegisterUtilities(spark)

    // sequence of rows
    val dates = fromDate.toLocalDate.toEpochDay.until(toDate.toLocalDate.toEpochDay).map(LocalDate.ofEpochDay).map(d => List(d.format(DateTimeFormatter.ISO_DATE)))

    // RDD
    val rdd = spark.sparkContext.parallelize(dates).map(a => Row.fromSeq(a))

    // Dataframe
    val df = spark.createDataFrame(rdd, new StructType().add(StructField("day", StringType, false)))

    val pair = "usdusd"
    df
      .withColumn("symbol", lit(pair))
      .withColumn("average_daily_price", lit(1.0))
      .write
      .option("compression", "gzip")
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/daily-prices/${currentTime}_average_daily_price_${pair}_${fromDate.format(DateTimeFormatter.ISO_DATE)}_${toDate.format(DateTimeFormatter.ISO_DATE)}.csv.gz")
      //.show()

    spark.stop()
  }
}