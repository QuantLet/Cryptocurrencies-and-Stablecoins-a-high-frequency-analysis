package Aggregators

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}

abstract class OrderBookAggregator(typeColumn : String, priceColumn : String, amountColumn : String, tradeSizeColumn : String, timeColumn : String) extends Aggregator[Row, OrderBook, Option[Double]] {

  def finish(reduction: OrderBook): Option[Double]

  override def zero: OrderBook = new OrderBook()

  override def reduce(buffer: OrderBook, data: Row): OrderBook = {
    val quoteType = data.getAs[String](typeColumn)
    val price = data.getAs[Double](priceColumn)
    val amount = data.getAs[Double](amountColumn)
    val tradeSize = data.getAs[Double](tradeSizeColumn)
    val time = data.getAs[Long](timeColumn)
    buffer.addQuote(quoteType, price, amount, time)
    buffer.addTradeSize(tradeSize)
    buffer
  }

  override def merge(b1: OrderBook, b2: OrderBook): OrderBook = {
    b1.add(b2)
    b1
  }

  override def bufferEncoder: Encoder[OrderBook] = Encoders.kryo[OrderBook]

  override def outputEncoder: Encoder[Option[Double]] = Encoders.product
}




