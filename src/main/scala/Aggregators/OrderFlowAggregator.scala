package Aggregators

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

object OrderFlowAggregator extends Aggregator[Row, Double, Double] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Double = 0

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Double, data: Row): Double = {
    val sign = if (data.getAs[Boolean]("sell")) -1 else 1
    val amount = data.getAs[Double]("amount")
    buffer + sign*amount
  }

  // Merge two intermediate values
  def merge(b1: Double, b2: Double): Double = {
    b1 + b2
  }

  // Transform the output of the reduction
  def finish(reduction: Double): Double = reduction

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Double] = Encoders.scalaDouble

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
