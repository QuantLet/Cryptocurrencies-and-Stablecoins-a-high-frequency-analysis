package Aggregators

import scala.collection.mutable.ListBuffer

class OrderBook {
  def averageTradeSize() : Double = tradeSizes.last // assuming all tradeSizes are the same, just taking the last one

  def liquidityImbalanceSell(multiplier: Double): Double = {
    return -midpoint().get + calculateAverageWeightedPrice(multiplier * averageTradeSize(), asks.sorted)
  }

  def liquidityImbalanceBuy(multiplier: Double) : Double = {
    return midpoint().get - calculateAverageWeightedPrice(multiplier * averageTradeSize(), bids.sorted.reverse)
  }

  def liquidityImbalance(multiplier: Double): Option[Double] = {
    if(emptyQuotes){
      return None
    }
    val imbalance = (liquidityImbalanceBuy(multiplier) - liquidityImbalanceSell(multiplier)) / (liquidityImbalanceBuy(multiplier) + liquidityImbalanceSell(multiplier))
    return Some(imbalance)
  }

  def midpoint() : Option[Double] = {
    if(emptyQuotes){
      return None
    }
    val midPoint = 0.5 * (bids.max.price + asks.min.price)
    return Some(midPoint)
  }

  def bidAskSpread(): Option[Double] = {
    if(emptyQuotes){
      return None
    }
    val spread = asks.min.price - bids.max.price
    return Some(spread)
  }

  private def emptyQuotes = {
    asks.isEmpty || bids.isEmpty
  }

  private def calculateAverageWeightedPrice(quantity: Double, sortedQuotes: ListBuffer[Quote]) = {
    val cumulativeQuotedAmount = sortedQuotes.map(q => q.amount).scanLeft(0.0)(_ + _).filter(a => a < quantity)
    val remainingQuantity = quantity - cumulativeQuotedAmount.last
    val usedQuotes = sortedQuotes.take(cumulativeQuotedAmount.length)
    (usedQuotes.take(usedQuotes.length - 1).map(q => q.price * q.amount).sum + remainingQuantity * usedQuotes.last.price) / quantity
  }

  def addTradeSize(size: Double) = {
    tradeSizes += size
  }

  def addQuotes(quotes: ListBuffer[Quote], quoteType : String) = {
    for(quote <- quotes){
      addQuote(quoteType, quote.price, quote.amount, quote.time)
    }
  }

  def add(other: OrderBook): Unit = {
    addQuotes(other.bids, "b")
    addQuotes(other.asks, "a")
    tradeSizes ++= other.tradeSizes
  }

  def addQuote(quoteType: String, price: Double, amount: Double, time : Long) = {
    if (time < minTime){
      minTime = time
      bids.clear()
      asks.clear()
      addQuoteInternal(quoteType, price, amount, time)
    } else if (time == minTime){
      addQuoteInternal(quoteType, price, amount, time)
    }
  }

  private def addQuoteInternal(quoteType: String, price: Double, amount: Double, time: Long) = {
    if (quoteType == "b") {
      bids += new Quote(price, amount, time)
    } else {
      asks += new Quote(price, amount, time)
    }
  }

  val bids = new ListBuffer[Quote]()
  val asks = new ListBuffer[Quote]()
  val tradeSizes = ListBuffer[Double]()

  var minTime = Long.MaxValue
}
