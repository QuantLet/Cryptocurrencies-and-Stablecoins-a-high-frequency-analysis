package Aggregators

case class TriangulationArbitrageSpreadCalculator(
                                      var cryptoStablePrice: Option[Double],
                                      var cryptoUsdPrice: Option[Double],
                                      var stableUsdPrice: Option[Double]) {

  def sellArbitrageSpread(): Double = {
    if (allValidValues) positivePart(cryptoUsdPrice.get / cryptoStablePrice.get - stableUsdPrice.get) else 0
  }

  def buyArbitrageSpread(): Double = {
    if (allValidValues) positivePart(stableUsdPrice.get - cryptoUsdPrice.get / cryptoStablePrice.get) else 0
  }

  private def positivePart(spread: Double): Double = {
    return if (spread > 0) spread else 0
  }

  private def allValidValues = {
    cryptoStablePrice.isDefined && cryptoStablePrice.get != 0 &&
      cryptoUsdPrice.isDefined && cryptoUsdPrice.get != 0 &&
      stableUsdPrice.isDefined && stableUsdPrice.get != 0
  }
}
