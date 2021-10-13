package Aggregators

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

class PercentageOfZerosAggregator(val columnName : String) extends Aggregator[Row, ZeroCounter, Double] {
  override def zero: ZeroCounter = ZeroCounter(0, 0)

  override def reduce(buffer: ZeroCounter, data: Row): ZeroCounter = {
    if(data.getAs[Double](this.columnName) == 0){
      buffer.zeros = buffer.zeros + 1
    }
    buffer.total = buffer.total + 1
    return buffer
  }

  override def merge(b1: ZeroCounter, b2: ZeroCounter): ZeroCounter = {
    return ZeroCounter(b1.zeros + b2.zeros, b1.total + b2.total)
  }

  override def finish(reduction: ZeroCounter): Double = {
    if (reduction.total == 0) 0 else reduction.zeros.toDouble / reduction.total.toDouble
  }

  override def bufferEncoder: Encoder[ZeroCounter] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

case class ZeroCounter(var zeros: Long, var total: Long)
