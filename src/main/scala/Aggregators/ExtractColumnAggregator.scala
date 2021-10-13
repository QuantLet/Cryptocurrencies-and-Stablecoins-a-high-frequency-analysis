package Aggregators

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

class ExtractColumnAggregator(column: String) extends Aggregator[Row, Double, Double]{
  override def zero: Double = Double.NaN

  override def reduce(buffer: Double, data: Row): Double = {
    val value = data.getAs[Double](column)
    if(!buffer.isNaN && buffer != value){
      throw new IllegalArgumentException
    }
    return value
  }

  override def merge(b1: Double, b2: Double): Double = {
    if(!b1.isNaN && !b2.isNaN && b1 != b2){
      throw new IllegalArgumentException
    }
    return if(b1.isNaN) b2 else b1
  }

  override def finish(reduction: Double): Double = reduction

  override def bufferEncoder: Encoder[Double] = Encoders.scalaDouble

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
