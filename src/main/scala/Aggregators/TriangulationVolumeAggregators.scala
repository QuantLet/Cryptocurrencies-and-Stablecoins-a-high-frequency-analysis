package Aggregators

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

class SellTriangulationVolumeAggregator(cryptoStablePair : String, cryptoUsdPair : String, stableUsdPair : String)
  extends TriangulationVolumeAggregator(cryptoStablePair, cryptoUsdPair, stableUsdPair) {

  override def reduce(buffer: TriangulationVolumeCalculator, data: Row): TriangulationVolumeCalculator = {
    val pair = data.getAs[String]("symbol")
    pair match {
      case this.cryptoStablePair => {
        buffer.cryptoStableVolume = Some(data.getAs[Double]("volume_of_min_price_trades_ask"))
        buffer.avgCryptoStablePrice = Some(data.getAs[Double]("avg_price"))
      }
      case this.cryptoUsdPair => buffer.cryptoUsdVolume = Some(data.getAs[Double]("volume_of_max_price_trades_bid"))
      case this.stableUsdPair => buffer.stableUsdVolume = Some(data.getAs[Double]("volume_of_min_price_trades_ask"))
      case _ => throw new Exception(s"Unrecognized symbol ${pair}")
    }
    buffer
  }
}

class BuyTriangulationVolumeAggregator(cryptoStablePair : String, cryptoUsdPair : String, stableUsdPair : String)
  extends TriangulationVolumeAggregator(cryptoStablePair, cryptoUsdPair, stableUsdPair) {

  override def reduce(buffer: TriangulationVolumeCalculator, data: Row): TriangulationVolumeCalculator = {
    val pair = data.getAs[String]("symbol")
    pair match {
      case this.cryptoStablePair => {
        buffer.cryptoStableVolume = Some(data.getAs[Double]("volume_of_max_price_trades_bid"))
        buffer.avgCryptoStablePrice = Some(data.getAs[Double]("avg_price"))
      }
      case this.cryptoUsdPair => buffer.cryptoUsdVolume = Some(data.getAs[Double]("volume_of_min_price_trades_ask"))
      case this.stableUsdPair => buffer.stableUsdVolume = Some(data.getAs[Double]("volume_of_max_price_trades_bid"))
      case _ => throw new Exception(s"Unrecognized symbol ${pair}")
    }
    buffer
  }
}

class TotalTriangulationVolumeAggregator(cryptoStablePair : String, cryptoUsdPair : String, stableUsdPair : String)
  extends TriangulationVolumeAggregator(cryptoStablePair, cryptoUsdPair, stableUsdPair) {

  override def reduce(buffer: TriangulationVolumeCalculator, data: Row): TriangulationVolumeCalculator = {
    val pair = data.getAs[String]("symbol")
    pair match {
      case this.cryptoStablePair => {
        buffer.cryptoStableVolume = Some(data.getAs[Double]("total_volume"))
        buffer.avgCryptoStablePrice = Some(data.getAs[Double]("avg_price"))
      }
      case this.cryptoUsdPair => buffer.cryptoUsdVolume = Some(data.getAs[Double]("total_volume"))
      case this.stableUsdPair => buffer.stableUsdVolume = Some(data.getAs[Double]("total_volume"))
      case _ => throw new Exception(s"Unrecognized symbol ${pair}")
    }
    buffer
  }
}

abstract class TriangulationVolumeAggregator(cryptoStablePair : String, cryptoUsdPair : String, stableUsdPair : String) extends Aggregator[Row, TriangulationVolumeCalculator, Double] {

  // abstract methods
  def reduce(buffer: TriangulationVolumeCalculator, data: Row): TriangulationVolumeCalculator


  override def zero: TriangulationVolumeCalculator = TriangulationVolumeCalculator(None, None, None, None)

  override def merge(b1: TriangulationVolumeCalculator, b2: TriangulationVolumeCalculator): TriangulationVolumeCalculator = {
    b1.cryptoStableVolume = b1.cryptoStableVolume.orElse(b2.cryptoStableVolume)
    b1.cryptoUsdVolume = b1.cryptoUsdVolume.orElse(b2.cryptoUsdVolume)
    b1.stableUsdVolume = b1.stableUsdVolume.orElse(b2.stableUsdVolume)
    b1.avgCryptoStablePrice = b1.avgCryptoStablePrice.orElse(b2.avgCryptoStablePrice)
    b1
  }

  def finish(reduction: TriangulationVolumeCalculator): Double = reduction.volume()

  override def bufferEncoder: Encoder[TriangulationVolumeCalculator] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
