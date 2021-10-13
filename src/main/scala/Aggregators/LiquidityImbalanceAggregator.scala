package Aggregators

class LiquidityImbalanceAggregator(typeColumn: String, priceColumn: String, amountColumn: String, avgTradeSizeColumn: String, multiplier: Double, timeColumn : String) extends OrderBookAggregator(typeColumn, priceColumn, amountColumn, avgTradeSizeColumn, timeColumn) {

  override def finish(reduction: OrderBook): Option[Double] = {
    reduction.liquidityImbalance(multiplier)
  }
}
