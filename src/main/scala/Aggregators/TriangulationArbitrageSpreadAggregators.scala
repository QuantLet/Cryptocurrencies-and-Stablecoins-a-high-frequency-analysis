package Aggregators

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

class SellArbitrageSpreadAggregator(cryptoStablePair : String, cryptoUsdPair : String, stableUsdPair : String)
  extends ArbitrageSpreadAggregator(cryptoStablePair, cryptoUsdPair, stableUsdPair) {

  override def reduce(buffer: TriangulationArbitrageSpreadCalculator, data: Row): TriangulationArbitrageSpreadCalculator = {
    val pair = data.getAs[String]("symbol")
    pair match {
      case this.cryptoStablePair => buffer.cryptoStablePrice = Some(data.getAs[Double]("min_price_trades_ask"))
      case this.cryptoUsdPair => buffer.cryptoUsdPrice = Some(data.getAs[Double]("max_price_trades_bid"))
      case this.stableUsdPair => buffer.stableUsdPrice = Some(data.getAs[Double]("min_price_trades_ask"))
      case _ => throw new Exception(s"Unrecognized symbol ${pair}")
    }
    buffer
  }

  override def finish(reduction: TriangulationArbitrageSpreadCalculator): Double = reduction.sellArbitrageSpread()
}

class BuyArbitrageSpreadAggregator(cryptoStablePair : String, cryptoUsdPair : String, stableUsdPair : String)
  extends ArbitrageSpreadAggregator(cryptoStablePair, cryptoUsdPair, stableUsdPair) {

  override def reduce(buffer: TriangulationArbitrageSpreadCalculator, data: Row): TriangulationArbitrageSpreadCalculator = {
    val pair = data.getAs[String]("symbol")
    pair match {
      case this.cryptoStablePair => buffer.cryptoStablePrice = Some(data.getAs[Double]("max_price_trades_bid"))
      case this.cryptoUsdPair => buffer.cryptoUsdPrice = Some(data.getAs[Double]("min_price_trades_ask"))
      case this.stableUsdPair => buffer.stableUsdPrice = Some(data.getAs[Double]("max_price_trades_bid"))
      case _ => throw new Exception(s"Unrecognized symbol ${pair}")
    }
    buffer
  }

  override def finish(reduction: TriangulationArbitrageSpreadCalculator): Double = reduction.buyArbitrageSpread()
}

abstract class ArbitrageSpreadAggregator(cryptoStablePair : String, cryptoUsdPair : String, stableUsdPair : String) extends Aggregator[Row, TriangulationArbitrageSpreadCalculator, Double] {

  // abstract methods
  def reduce(buffer: TriangulationArbitrageSpreadCalculator, data: Row): TriangulationArbitrageSpreadCalculator
  def finish(reduction: TriangulationArbitrageSpreadCalculator): Double


  override def zero: TriangulationArbitrageSpreadCalculator = TriangulationArbitrageSpreadCalculator(None, None, None)

  override def merge(b1: TriangulationArbitrageSpreadCalculator, b2: TriangulationArbitrageSpreadCalculator): TriangulationArbitrageSpreadCalculator = {
    b1.cryptoStablePrice = b1.cryptoStablePrice.orElse(b2.cryptoStablePrice)
    b1.cryptoUsdPrice = b1.cryptoUsdPrice.orElse(b2.cryptoUsdPrice)
    b1.stableUsdPrice = b1.stableUsdPrice.orElse(b2.stableUsdPrice)
    b1
  }

  override def bufferEncoder: Encoder[TriangulationArbitrageSpreadCalculator] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}


