package Aggregators

import org.apache.spark.sql.{Encoder, Encoders, Row}

object ExchangeOfMaxPriceBidAggregator extends ColumnOfMaxPriceBidAggregator[String] {

  override def zero: Extrema[String] = Extrema(Double.MinPositiveValue, "")

  override def outputEncoder: Encoder[String] = Encoders.STRING

  override def bufferEncoder: Encoder[Extrema[String]] = Encoders.product

  override def extractKey(data: Row): String = {
    data.getAs[String]("exchange")
  }
}

object VolumeOfMaxPriceBidAggregator extends ColumnOfMaxPriceBidAggregator[Double] {

  override def zero: Extrema[Double] = Extrema(Double.MinPositiveValue, 0)

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  override def bufferEncoder: Encoder[Extrema[Double]] = Encoders.product

  override def extractKey(data: Row): Double = {
    data.getAs[Double]("volume_trades_bid")
  }
}

object OrderFlowOfMaxPriceBidAggregator extends ColumnOfMaxPriceBidAggregator[Double] {

  override def zero: Extrema[Double] = Extrema(Double.MinPositiveValue, 0)

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  override def bufferEncoder: Encoder[Extrema[Double]] = Encoders.product

  override def extractKey(data: Row): Double = {
    data.getAs[Double]("order_flow")
  }
}
