package Aggregators

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

class ReturnAggregator(openColumn : String, closeColumn : String) extends Aggregator[Row, OpenCloseTracker, Double]  {
  override def zero: OpenCloseTracker = new OpenCloseTracker(Double.MaxValue, Double.MinPositiveValue)

  override def reduce(buffer: OpenCloseTracker, data: Row): OpenCloseTracker = {
    val openValue = data.getAs[Double](openColumn)
    val closeValue = data.getAs[Double](closeColumn)
    if(openValue < buffer.minOpen){
      buffer.minOpen = openValue
    }
    if(closeValue > buffer.maxClose){
      buffer.maxClose = closeValue
    }
    buffer
  }

  override def merge(b1: OpenCloseTracker, b2: OpenCloseTracker): OpenCloseTracker = {
    new OpenCloseTracker(Math.min(b1.minOpen, b2.minOpen), Math.max(b1.maxClose, b2.maxClose))
  }

  override def finish(reduction: OpenCloseTracker): Double = {
      return (reduction.maxClose - reduction.minOpen) / reduction.minOpen
  }

  override def bufferEncoder: Encoder[OpenCloseTracker] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

case class OpenCloseTracker(var minOpen : Double, var maxClose : Double)
