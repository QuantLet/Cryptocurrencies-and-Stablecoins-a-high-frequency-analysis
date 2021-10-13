package main.scala

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SparkGenerateMinutesSequence {
  def main(args: Array[String]): Unit = {
    val from = Utilities.ToUnixTimestampMs(args(0))
    val to = Utilities.ToUnixTimestampMs(args(1))
    val numberMinutes = args(2).toInt // minutes
    val s3Bucket = if (args.length == 4) args(3) else ""
    val currentTime = LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("uuuu-MM-dd__HH-mm-ss"))

    val spark = SparkSession
      .builder()
      .appName("Spark GenerateMinutesSequence")
      .getOrCreate()
    Utilities.RegisterUtilities(spark)

    // sequence of rows
    val msInMinute = 1000 * 60
    val startInterval = Seq.range(from, to, numberMinutes * msInMinute).map(n => Row(n))

    // RDD
    val rdd = spark.sparkContext.parallelize(startInterval)

    // Dataframe
    val df = spark.createDataFrame(rdd, new StructType().add(StructField("start", LongType, false)))

    df
      .withColumn("start_display", expr("DisplayEpochMs(start)"))
      .write
      .option("compression", "gzip")
      .option("header", "true")
      .csv(s"${s3Bucket}data/processed-data/utilities/${currentTime}_complete_minute_sequence_${Utilities.ToDate(from)}_${Utilities.ToDate(to)}_${numberMinutes}m.csv.gz")
  }
}
