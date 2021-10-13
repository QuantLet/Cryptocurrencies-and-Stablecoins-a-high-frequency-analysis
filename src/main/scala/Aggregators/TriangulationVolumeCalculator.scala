package Aggregators

case class TriangulationVolumeCalculator(var cryptoStableVolume : Option[Double],
                                         var cryptoUsdVolume : Option[Double],
                                         var stableUsdVolume : Option[Double],
                                         var avgCryptoStablePrice : Option[Double]) {
  def volume(): Double = {
    if(allValidValues) List(cryptoStableVolume.get*avgCryptoStablePrice.get, cryptoUsdVolume.get*avgCryptoStablePrice.get, stableUsdVolume.get).min else 0
  }

  private def allValidValues = {
    cryptoStableVolume.isDefined && cryptoStableVolume.get != 0 &&
      cryptoUsdVolume.isDefined && cryptoUsdVolume.get != 0 &&
      stableUsdVolume.isDefined && stableUsdVolume.get != 0 &&
      avgCryptoStablePrice.isDefined && avgCryptoStablePrice.get != 0
  }
}
