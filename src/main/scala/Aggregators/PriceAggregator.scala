package Aggregators

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

case class Average(var sum: Double, var volume: Double)

abstract class PriceAggregator extends Aggregator[Row, Average, Double] {

  // condition to include price
  def shouldIncludePrice(data : Row): Boolean

  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Average = Average(0, 0L)

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Average, data: Row): Average = {
    if(shouldIncludePrice(data)){
      val volume = data.getAs[Double]("amount")
      val price = data.getAs[Double]("price")
      buffer.sum += price*volume
      buffer.volume += volume
    }
    buffer
  }

  // Merge two intermediate values
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.volume += b2.volume
    b1
  }

  // Transform the output of the reduction
  def finish(reduction: Average): Double = reduction.sum / reduction.volume

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Average] = Encoders.product

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
