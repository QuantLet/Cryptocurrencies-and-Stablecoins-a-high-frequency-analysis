package Aggregators

class BidAskSpreadAggregator(typeColumn : String, priceColumn : String, amountColumn : String, tradeSizeColumn : String, timeColumn : String) extends OrderBookAggregator(typeColumn, priceColumn, amountColumn, tradeSizeColumn, timeColumn) {
  override def finish(reduction: OrderBook): Option[Double] = reduction.bidAskSpread()
}
