package Aggregators

class TriangulationAggregatorFactory(cryptoStablePair: String, cryptoUsdPair: String, stableUsdPair: String) {

  def Symbol(): String = s"${cryptoStablePair}-${cryptoUsdPair}-${stableUsdPair}"

  def AvgStableUsdPrice(): TriangulationAvgPriceAggregator = {
    return new TriangulationAvgPriceAggregator(stableUsdPair)
  }

  def AvgCryptoUsdPrice(): TriangulationAvgPriceAggregator = {
    return new TriangulationAvgPriceAggregator(cryptoUsdPair)
  }

  def AvgCryptoStablePrice(): TriangulationAvgPriceAggregator = {
    return new TriangulationAvgPriceAggregator(cryptoStablePair)
  }

  def TotalVolumeAggregator(): TotalTriangulationVolumeAggregator = {
    return new TotalTriangulationVolumeAggregator(cryptoStablePair, cryptoUsdPair, stableUsdPair)
  }

  def BuyArbitrageSpreadAggregator(): BuyArbitrageSpreadAggregator = {
    return new BuyArbitrageSpreadAggregator(cryptoStablePair, cryptoUsdPair, stableUsdPair)
  }

  def SellArbitrageSpreadAggregator(): SellArbitrageSpreadAggregator = {
    return new SellArbitrageSpreadAggregator(cryptoStablePair, cryptoUsdPair, stableUsdPair)
  }

  def BuyVolumeAggregator(): BuyTriangulationVolumeAggregator = {
    return new BuyTriangulationVolumeAggregator(cryptoStablePair, cryptoUsdPair, stableUsdPair)
  }

  def SellVolumeAggregator(): SellTriangulationVolumeAggregator = {
    return new SellTriangulationVolumeAggregator(cryptoStablePair, cryptoUsdPair, stableUsdPair)
  }
}
