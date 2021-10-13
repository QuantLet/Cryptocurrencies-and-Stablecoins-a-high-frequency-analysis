package Aggregators

import org.apache.spark.sql.{Encoder, Encoders, Row}

object ExchangeOfMinPriceAskAggregator extends ColumnOfMinPriceAskAggregator[String] {

  override def zero: Extrema[String] = Extrema(Double.MaxValue, "")

  override def outputEncoder: Encoder[String] = Encoders.STRING

  override def bufferEncoder: Encoder[Extrema[String]] = Encoders.product

  override def extractKey(data: Row): String = {
    data.getAs[String]("exchange")
  }
}

object VolumeOfMinPriceAskAggregator extends ColumnOfMinPriceAskAggregator[Double] {

  override def zero: Extrema[Double] = Extrema(Double.MaxValue, 0)

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  override def bufferEncoder: Encoder[Extrema[Double]] = Encoders.product

  override def extractKey(data: Row): Double = {
    data.getAs[Double]("volume_trades_ask")
  }
}

object OrderFlowOfMinPriceAskAggregator extends ColumnOfMinPriceAskAggregator[Double] {

  override def zero: Extrema[Double] = Extrema(Double.MaxValue, 0)

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  override def bufferEncoder: Encoder[Extrema[Double]] = Encoders.product

  override def extractKey(data: Row): Double = {
    data.getAs[Double]("order_flow")
  }
}
