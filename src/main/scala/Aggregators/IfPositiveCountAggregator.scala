package Aggregators

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

class IfPositiveCountAggregator(val columnName : String) extends Aggregator[Row, Long, Long] {

  // condition to include price
  def shouldIncludeValue(data : Row): Boolean = {
    return data.getAs[Double](this.columnName) > 0
  }

  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Long = 0L

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Long, data: Row): Long = {
    return if(shouldIncludeValue(data)) buffer + 1 else buffer
  }

  // Merge two intermediate values
  def merge(b1: Long, b2: Long): Long = {
    return b1 + b2
  }

  // Transform the output of the reduction
  def finish(reduction: Long): Long = reduction

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Long] = Encoders.scalaLong

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Long] = Encoders.scalaLong
}