package Aggregators

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

class IfNotNullAverageAggregator(columnName : String) extends Aggregator[Row, SimpleAverage, Double] {

  // condition to include price
  def shouldIncludeValue(data : Row): Boolean = {
    val index = data.fieldIndex(columnName)
    return !data.isNullAt(index)
  }

  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: SimpleAverage = SimpleAverage(0, 0)

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: SimpleAverage, data: Row): SimpleAverage = {
    if(shouldIncludeValue(data)){
      val value = data.getAs[Double](this.columnName)
      buffer.sum += value
      buffer.count += 1
    }
    buffer
  }

  // Merge two intermediate values
  def merge(b1: SimpleAverage, b2: SimpleAverage): SimpleAverage = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // Transform the output of the reduction
  def finish(reduction: SimpleAverage): Double = if(reduction.count == 0) 0 else reduction.sum / reduction.count

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[SimpleAverage] = Encoders.product

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
