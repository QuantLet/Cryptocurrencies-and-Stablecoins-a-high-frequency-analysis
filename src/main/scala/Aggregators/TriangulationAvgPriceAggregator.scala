package Aggregators

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

class TriangulationAvgPriceAggregator(desiredPair : String) extends Aggregator[Row, Double, Double] {
  override def zero: Double = 0

  override def reduce(buffer: Double, data: Row): Double = {
    val pair = data.getAs[String]("symbol")
    if(pair == desiredPair){
      return data.getAs[Double]("avg_price")
    }
    buffer
  }

  override def merge(b1: Double, b2: Double): Double = {
    return if(b1 != 0) b1 else b2
  }

  override def finish(reduction: Double): Double = reduction //TODO this might be zero if original value was null, be careful!

  override def bufferEncoder: Encoder[Double] = Encoders.scalaDouble

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
