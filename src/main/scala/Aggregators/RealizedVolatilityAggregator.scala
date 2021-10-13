package Aggregators

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

class RealizedVolatilityAggregator(column : String) extends Aggregator[Row, Double, Double] {
  override def zero: Double = 0

  override def reduce(buffer: Double, data: Row): Double = {
    val financialReturn = data.getAs[Double](column)
    return buffer + financialReturn * financialReturn
  }

  override def merge(b1: Double, b2: Double): Double = {
    return b1 + b2
  }

  override def finish(reduction: Double): Double = math.sqrt(reduction)

  override def bufferEncoder: Encoder[Double] = Encoders.scalaDouble

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}


class RealizedVarianceAggregator(column : String) extends Aggregator[Row, Double, Double] {
  override def zero: Double = 0

  override def reduce(buffer: Double, data: Row): Double = {
    val financialReturn = data.getAs[Double](column)
    return buffer + financialReturn * financialReturn
  }

  override def merge(b1: Double, b2: Double): Double = {
    return b1 + b2
  }

  override def finish(reduction: Double): Double = reduction

  override def bufferEncoder: Encoder[Double] = Encoders.scalaDouble

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}