package main.scala

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import Aggregators.{ReturnPriceAggregator, PercentageOfZerosAggregator}
import org.apache.spark.sql
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, expr, kurtosis, lit, min, skewness, stddev, udf, when, max}

object Utilities {

  def RegisterUtilities(spark: SparkSession) = {
    spark.udf.register("DayOf", udf(Utilities.DayOf))
    spark.udf.register("DayOfMs", udf(Utilities.DayOfMs))
    spark.udf.register("YearAndMonthOf", udf(Utilities.YearAndMonthOf))
    spark.udf.register("YearAndMonthOfMs", udf(Utilities.YearAndMonthOfMs))
    spark.udf.register("YearAndMonthOfString", udf(Utilities.YearAndMonthOfString))
    spark.udf.register("MinuteOf", udf(Utilities.MinuteOf))
    spark.udf.register("MinuteOfAsEpoch", udf(Utilities.MinuteOfAsEpoch))
    spark.udf.register("CoinapiToEpochMs", udf(Utilities.CoinapiToEpochMs))
    spark.udf.register("DisplayEpochMs", udf(Utilities.DisplayEpochMs))
    spark.udf.register("TimeIntervalToEpochMs", udf(Utilities.TimeIntervalToEpochMs))
    spark.udf.register("TimeIntervalFromTsToEpochMs", udf(Utilities.TimeIntervalFromTsToEpochMs))
    spark.udf.register("MinuteOfTheDay", udf(Utilities.MinuteOfTheDay))
    spark.udf.register("MinuteOfTheDayMs", udf(Utilities.MinuteOfTheDayMs))
  }

  def DailyVolumeInUsd(volumeColumn: String): Column = when(!col("usd_rate_base_ccy_average_daily_price").isNull,
    col(volumeColumn)*col("usd_rate_base_ccy_average_daily_price")
  ).otherwise(col(volumeColumn)) // assume usd_rate equal to 1 (ok because it happens only for USDC and DAI)

  def OrderFlowInUsd(volumeColumn: String): Column = when(!col("usd_rate_base_ccy_average_daily_price").isNull,
    col(volumeColumn)*col("usd_rate_base_ccy_average_daily_price")
  ).otherwise(col(volumeColumn)) // assume usd_rate equal to 1 (ok because it happens only for USDC and DAI)

  def DailyArbitrageProfitUsd(arbitrageColumn: String): Column = when(!col("usd_rate_average_daily_price").isNull,
    col(arbitrageColumn)*col("usd_rate_average_daily_price")
  ).otherwise(col(arbitrageColumn)) // assume usd_rate equal to 1 (ok because it happens only for USDC and DAI)

  def CurrentTime() = {
    LocalDateTime.now(UTC).format(DateTimeFormatter.ofPattern("uuuu-MM-dd__HH-mm-ss"))
  }


  def ToDate(epoch: Long) = {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(epoch), UTC).format(DateTimeFormatter.ISO_DATE)
  }


  def ArbitrageProfit: Column = when(col("volume_of_min_price_trades_ask") - col("volume_of_max_price_trades_bid") < 0,
    col("arbitrage_spread")*col("volume_of_min_price_trades_ask")
  ).otherwise(col("arbitrage_spread")*col("volume_of_max_price_trades_bid"))


  def ExchangesAreDifferent = col("exchange_of_min_price_trades_ask").notEqual(col("exchange_of_max_price_trades_bid"))


  def PricesNotNaN = !col("min_price_trades_ask").isNaN && !col("max_price_trades_bid").isNaN

  def EthPaxAnomalousFilter = !col("start_second").contains("2019-04-27") || col("symbol") =!= lit("ethpax") //ETHPAX anomalous values

  def TusdUsdAnomalousFilter = (lit(0.1) < col("avg_price") && col("avg_price") < lit(500)) || col("symbol") =!= lit("tusdusd") //TUSDUSD anomalous value


  def ArbitrageSpread: Column = when( PricesNotNaN && ExchangesAreDifferent &&
    col("max_price_trades_bid") - col("min_price_trades_ask") > 0,
    col("max_price_trades_bid") - col("min_price_trades_ask"))
    .otherwise(0)


  def ArbitrageSpreadAsMidpointPercentage: Column = when(col("arbitrage_spread") > 0,
    lit(10000) * col("arbitrage_spread") / (lit(0.5)*(col("min_price_trades_ask") + col("max_price_trades_bid"))))
    .otherwise(0)


  def ToUnixTimestampMs(date: String): Long = {
    val localDate = LocalDate.parse(date, DateTimeFormatter.ISO_DATE)
    val dateTime = localDate.atStartOfDay(UTC)
    dateTime.toInstant().toEpochMilli()
  }

  def ToDateTimeMs(date: String): ZonedDateTime = {
    val localDate = LocalDate.parse(date, DateTimeFormatter.ISO_DATE)
    val dateTime = localDate.atStartOfDay(UTC)
    dateTime
  }

  def ToUnixTimestampMsDt(date: String): Long = {
    val localDate = ZonedDateTime.parse(date, DateTimeFormatter.ISO_DATE_TIME)
    localDate.toInstant().toEpochMilli()
  }

  val CoinapiToEpochMs: (String) => Long = (ts: String) => {
    val date = ZonedDateTime.parse(ts, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS").withZone(UTC))
    date.toInstant().toEpochMilli()
  }


  val TimeInterval: (Long, Int) => String = (unixTimestamp: Long, numberSeconds: Int) => {
    val date = ZonedDateTime.ofInstant(Instant.ofEpochMilli(unixTimestamp), UTC)
    val initialSecond: ZonedDateTime = TimeIntervalFromZonedDateTime(numberSeconds, date)
    initialSecond.format(DateTimeFormatter.ISO_INSTANT)
  }

  val TimeIntervalToEpochMs: (Long, Int) => Long = (unixTimestamp: Long, numberSeconds: Int) => {
    val date = ZonedDateTime.ofInstant(Instant.ofEpochMilli(unixTimestamp), UTC)
    val initialSecond: ZonedDateTime = TimeIntervalFromZonedDateTime(numberSeconds, date)
    initialSecond.toInstant().toEpochMilli
  }

  val TimeIntervalFromTsToEpochMs: (Long, Int) => Long = (unixTimestamp: Long, numberSeconds: Int) => {
    val date = ZonedDateTime.ofInstant(Instant.ofEpochSecond(unixTimestamp), UTC)
    val initialSecond: ZonedDateTime = TimeIntervalFromZonedDateTime(numberSeconds, date)
    initialSecond.toInstant().toEpochMilli
  }

  // numberSeconds cannot be greater than 60*60
  def TimeIntervalFromZonedDateTime(numberSeconds: Int, date: ZonedDateTime) = {
    val secondsFromDayStart = date.getHour * 60 * 60 + date.getMinute * 60 + date.getSecond
    val secondInterval = secondsFromDayStart / numberSeconds
    val residualSecondsWithinHour = secondInterval * numberSeconds % 3600
    val residualSecondsWithinMinute = residualSecondsWithinHour - residualSecondsWithinHour / 60 * 60
    val initialSecond = ZonedDateTime.of(date.getYear, date.getMonthValue, date.getDayOfMonth, date.getHour, residualSecondsWithinHour / 60, residualSecondsWithinMinute, 0, UTC)
    initialSecond
  }

  val MinuteOfTheDay: (Long, Int) => Int = (unixTimestamp: Long, numberMinutes: Int) => { // timestamp in seconds
    val date = ZonedDateTime.ofInstant(Instant.ofEpochSecond(unixTimestamp), UTC)
    getMinuteOfDay(numberMinutes, date)
  }

  private def getMinuteOfDay(numberMinutes: Int, date: ZonedDateTime) = {
    val minuteOfDay = date.getHour * 60 + date.getMinute
    minuteOfDay - (minuteOfDay % numberMinutes)
  }

  val MinuteOfTheDayMs: (Long, Int) => Int = (unixTimestamp: Long, numberMinutes: Int) => { // timestamp in milliseconds
    val date = ZonedDateTime.ofInstant(Instant.ofEpochMilli(unixTimestamp), UTC)
    getMinuteOfDay(numberMinutes, date)
  }

  val DisplayEpochMs: (Long) => String = (unixTimestamp: Long) => {
    val date = ZonedDateTime.ofInstant(Instant.ofEpochMilli(unixTimestamp), UTC)
    date.format(DateTimeFormatter.ISO_INSTANT)
  }

  val DayOf: (Long) => String = (unixTimestamp: Long) => { //unix timestamp in seconds
    val date = LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTimestamp), UTC)
    date.format(DateTimeFormatter.ISO_DATE)
  }

  val DayOfMs: (Long) => String = (unixTimestamp: Long) => { //unix timestamp in milliseconds
    val date = LocalDateTime.ofInstant(Instant.ofEpochMilli(unixTimestamp), UTC)
    date.format(DateTimeFormatter.ISO_DATE)
  }

  val DayOfTs: (String) => String = (timestamp: String) => {
    val date = LocalDateTime.ofInstant(Instant.parse(timestamp), UTC)
    date.format(DateTimeFormatter.ISO_DATE)
  }

  val MinuteOf: (Long) => String = (unixTimestamp: Long) => { //unix timestamp in seconds
    val date = ZonedDateTime.ofInstant(Instant.ofEpochSecond(unixTimestamp), UTC)
    val minute = ZonedDateTime.of(date.getYear, date.getMonthValue, date.getDayOfMonth, date.getHour, date.getMinute, 0, 0, UTC)
    minute.format(DateTimeFormatter.ISO_INSTANT)
  }

  val MinuteOfAsEpoch: (Long) => Long = (unixTimestamp: Long) => { //unix timestamp in seconds
    val date = ZonedDateTime.ofInstant(Instant.ofEpochSecond(unixTimestamp), UTC)
    val minute = ZonedDateTime.of(date.getYear, date.getMonthValue, date.getDayOfMonth, date.getHour, date.getMinute, 0, 0, UTC)
    minute.toEpochSecond
  }

  val YearAndMonthOf: (Long) => String = (unixTimestamp: Long) => { //unix timestamp in seconds
    val date = LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTimestamp), UTC)
    date.format(YearMonthFormatter)
  }

  val YearAndMonthOfMs: (Long) => String = (unixTimestamp: Long) => {
    val date = LocalDateTime.ofInstant(Instant.ofEpochMilli(unixTimestamp), UTC)
    date.format(YearMonthFormatter)
  }

  val YearAndMonthOfTs: (String) => String = (timestamp: String) => {
    val date = LocalDateTime.ofInstant(Instant.parse(timestamp), UTC)
    date.format(YearMonthFormatter)
  }

  val YearAndMonthOfString: (String) => String = (date: String) => {
    val localDate = LocalDate.parse(date, DateTimeFormatter.ISO_DATE)
    localDate.format(YearMonthFormatter)
  }

  def UTC = {
    ZoneId.of("UTC")
  }

  private def YearMonthFormatter = {
    DateTimeFormatter.ofPattern("uuuu-MM")
  }

  def CalculateAndSaveStatistics(dataSet: Dataset[Row], columnName: String, csvPath: String) = {
    val pair = dataSet.first().getAs[String]("symbol")

    dataSet
      .agg(
        avg(columnName).as("mean"),
        stddev(columnName).as("stddev"),
        skewness(columnName).as("skewness"),
        kurtosis(columnName).as("kurtosis"),
        min(columnName).as("min"),
        max(columnName).as("max"),
        new PercentageOfZerosAggregator(columnName).toColumn.as("percentage_of_zeros")
      )
      .withColumn("variation_coefficient", col("stddev") / col("mean"))
      .withColumn("type", lit(columnName))
      .withColumn("symbol", lit(pair))
      .write
      .option("header", true)
      .csv(csvPath)
  }

  def GetMonthlyVolatilityColumns(inputDf: DataFrame, priceColumn: String, filter: Column, prefix : String = "", csvPath : String = ""): sql.DataFrame = {
    val dfReturns = inputDf
      .selectExpr("unix_timestamp(start_second) as second", "MinuteOfAsEpoch(unix_timestamp(start_second)) as start_minute", "symbol", priceColumn)
      .where(filter)
      .where(!col(priceColumn).isNaN)
      .groupBy("start_minute", "symbol")
      .agg(
        new ReturnPriceAggregator(priceColumn, "second").toColumn.as("return")
      )
      .where(col("return") > -0.5 && col("return") < 0.5) // filtering excessive returns

    if(csvPath.nonEmpty) {
      CalculateAndSaveStatistics(dfReturns, "return", csvPath)
    }

    return dfReturns
      .withColumn("year_month", expr("YearAndMonthOf(start_minute)"))
      .groupBy("year_month")
      .agg(
        stddev("return").as(prefix + "volatility_minute_returns")
      )
      .withColumn(prefix + "volatility_minute_returns_monthly", lit(Math.sqrt(60 * 24 * 30)) * col(prefix + "volatility_minute_returns"))
  }
}
