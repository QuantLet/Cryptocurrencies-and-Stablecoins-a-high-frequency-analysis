package Aggregators

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

class LogReturnPriceAggregator(column : String, timeColumn : String) extends Aggregator[Row, OpenClosePriceTracker, Double]{
  override def zero: OpenClosePriceTracker = OpenClosePriceTracker(0, Long.MaxValue, 0, Long.MinValue)

  override def reduce(buffer: OpenClosePriceTracker, data: Row): OpenClosePriceTracker = {
    val price = data.getAs[Double](column)
    val time = data.getAs[Long](timeColumn)
    if(time < buffer.openEpoch){
      buffer.openEpoch = time
      buffer.openPrice = price
    }
    if(time > buffer.closeEpoch){
      buffer.closeEpoch = time
      buffer.closePrice = price
    }
    buffer
  }

  override def merge(b1: OpenClosePriceTracker, b2: OpenClosePriceTracker): OpenClosePriceTracker = {
    val openEpoch = Math.min(b1.openEpoch, b2.openEpoch)
    val openPrice = if (b1.openEpoch < b2.openEpoch) b1.openPrice else b2.openPrice
    val closeEpoch = Math.max(b1.closeEpoch, b2.closeEpoch)
    val closePrice = if (b1.closeEpoch > b2.closeEpoch) b1.closePrice else b2.closePrice
    return new OpenClosePriceTracker(openPrice, openEpoch, closePrice, closeEpoch)
  }

  override def finish(reduction: OpenClosePriceTracker): Double = {
    return (reduction.closePrice - reduction.openPrice) / reduction.openPrice
  }

  override def bufferEncoder: Encoder[OpenClosePriceTracker] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}