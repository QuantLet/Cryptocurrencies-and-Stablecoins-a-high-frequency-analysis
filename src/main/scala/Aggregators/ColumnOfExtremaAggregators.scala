package Aggregators

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

class ColumnOfMinDateAggregator(outputColumn : String) extends ColumnOfExtremaAggregators[Double,Long](outputColumn, "date") {
  override def shouldUpdateExtrema(newValue: Long, currentExtrema: Long): Boolean = {
    return newValue < currentExtrema
  }

  override def zero: Extreme[Double, Long] = Extreme(0, Long.MaxValue)

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  override def bufferEncoder: Encoder[Extreme[Double, Long]] = Encoders.product
}

class ColumnOfMaxDateAggregator(outputColumn : String) extends ColumnOfExtremaAggregators[Double,Long](outputColumn, "date") {
  override def shouldUpdateExtrema(newValue: Long, currentExtrema: Long): Boolean = {
    return newValue > currentExtrema
  }

  override def zero: Extreme[Double, Long] = Extreme(0, Long.MinValue)

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  override def bufferEncoder: Encoder[Extreme[Double, Long]] = Encoders.product
}

abstract class ColumnOfExtremaAggregators[T1,T2](outputColumn : String, optimizationColumn : String) extends Aggregator[Row, Extreme[T1,T2],T1] {

  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Extreme[T1, T2]

  def shouldUpdateExtrema(newValue: T2, currentExtrema: T2): Boolean

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Extreme[T1,T2], data: Row): Extreme[T1,T2] = {
    val value = data.getAs[T1](outputColumn)
    val optimizationValue = data.getAs[T2](optimizationColumn)
    if(shouldUpdateExtrema(optimizationValue, buffer.optimizationValue)){
      buffer.optimizationValue = optimizationValue
      buffer.outputValue = value
    }
    buffer
  }

  // Merge two intermediate values
  def merge(b1: Extreme[T1,T2], b2: Extreme[T1,T2]): Extreme[T1,T2] = {
    if(shouldUpdateExtrema(b1.optimizationValue, b2.optimizationValue)){
      b1
    }else{
      b2
    }
  }

  // Transform the output of the reduction
  def finish(reduction: Extreme[T1,T2]): T1 = reduction.outputValue

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[T1]

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Extreme[T1, T2]]
}

case class Extreme[T1, T2](var outputValue : T1, var optimizationValue : T2)
