package Aggregators

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Row}

case class Extrema[A](var value: Double, var key: A)

abstract class ColumnOfMinPriceAskAggregator[A] extends ColumnOfExtremaPriceAggregator[A] {

  def shouldUpdateExtrema(newValue: Double, currentExtrema: Double): Boolean = {
    newValue < currentExtrema
  }

  def extractPrice(data: Row): Double = {
    data.getAs[Double]("price_trades_ask")
  }

}

abstract class ColumnOfMaxPriceBidAggregator[A] extends ColumnOfExtremaPriceAggregator[A] {

  def shouldUpdateExtrema(newValue: Double, currentExtrema: Double): Boolean = {
    newValue > currentExtrema
  }

  def extractPrice(data: Row): Double = {
    data.getAs[Double]("price_trades_bid")
  }

}

abstract class ColumnOfExtremaPriceAggregator[A] extends Aggregator[Row, Extrema[A], A] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Extrema[A]

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[A]

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Extrema[A]]

  def extractKey(data: Row): A

  def shouldUpdateExtrema(newValue: Double, currentExtrema: Double): Boolean

  def extractPrice(data: Row): Double

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Extrema[A], data: Row): Extrema[A] = {
    val price = extractPrice(data)
    if(!price.isNaN && shouldUpdateExtrema(price, buffer.value)){
    buffer.value = price
    buffer.key = extractKey(data)
  }
    buffer
  }

  // Merge two intermediate values
  def merge(b1: Extrema[A], b2: Extrema[A]): Extrema[A] = {
    if(shouldUpdateExtrema(b1.value, b2.value)){
      b1
    }else{
      b2
    }
  }

  // Transform the output of the reduction
  def finish(reduction: Extrema[A]): A = reduction.key
}
