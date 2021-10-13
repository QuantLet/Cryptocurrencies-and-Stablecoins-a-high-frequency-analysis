import Aggregators.{OrderBook, Quote}
import org.scalatest.FunSuite
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, not}

class OrderBookTests extends FunSuite {

  test("AddQuote") {
    val orderBook = new OrderBook()
    orderBook.addQuote("b", 10, 2, 0)
    orderBook.addQuote("a", 20, 5, 0)

    orderBook.bids should contain (new Quote(10, 2, 0))
    orderBook.asks should contain (new Quote(20, 5, 0))
  }

  test("Add") {
    val o1 = new OrderBook()
    o1.addQuote("b", 10, 2, 0)
    o1.addQuote("a", 20, 5, 0)
    o1.addTradeSize(1000)

    val o2 = new OrderBook()
    o2.addQuote("b", 100, 2, 0)
    o2.addQuote("a", 200, 5, 0)
    o2.addTradeSize(2000)

    o1.add(o2)

    o1.bids should contain (new Quote(10, 2, 0))
    o1.asks should contain (new Quote(20, 5, 0))
    o1.bids should contain (new Quote(100, 2, 0))
    o1.asks should contain (new Quote(200, 5, 0))
    o1.tradeSizes should contain (1000)
    o1.tradeSizes should contain (2000)
  }

  test("BidAskSpread") {
    val orderBook = new OrderBook()
    orderBook.addQuote("b", 15, 2, 0)
    orderBook.addQuote("b", 10, 2, 0)

    orderBook.addQuote("a", 18, 5, 0)
    orderBook.addQuote("a", 20, 5, 0)

    assert(orderBook.bidAskSpread() == 3)
  }

  test("AddTradeSize") {
    val orderBook = new OrderBook()
    orderBook.addTradeSize(100)
    orderBook.addTradeSize(200)

    orderBook.tradeSizes should contain (100)
    orderBook.tradeSizes should contain (200)
  }

  test("LiquidityImbalanceBuy") {
    val orderBook = new OrderBook()
    orderBook.addQuote("a", 20, 200, 0)
    orderBook.addQuote("b", 16, 100, 0)
    orderBook.addQuote("b", 10, 100, 0)
    orderBook.addTradeSize(10)

    val midpoint = 0.5 * (20 + 16)

    assert(midpoint - 16.0 * 10.0 / 10.0  == orderBook.liquidityImbalanceBuy(1))
    assert(midpoint - 16.0 * 20.0 / 20.0  == orderBook.liquidityImbalanceBuy(2))
    assert(midpoint - (16.0 * 100.0 + 10.0 * 50.0) / 150.0  == orderBook.liquidityImbalanceBuy(15))
  }

  test("LiquidityImbalanceSell") {
    val orderBook = new OrderBook()
    orderBook.addQuote("a", 23, 200, 0)
    orderBook.addQuote("a", 20, 200, 0)
    orderBook.addQuote("b", 16, 100, 0)
    orderBook.addQuote("b", 10, 100, 0)
    orderBook.addTradeSize(10)

    val midpoint = 0.5 * (20 + 16)

    assert(-midpoint + 20.0 * 10.0 / 10.0  == orderBook.liquidityImbalanceSell(1))
    assert(-midpoint + 20.0 * 20.0 / 20.0  == orderBook.liquidityImbalanceSell(2))
    assert(-midpoint + (20.0 * 200.0 + 23.0 * 150.0) / 350.0  == orderBook.liquidityImbalanceSell(35))
  }

  test("LiquidityImbalance") {
    val orderBook = new OrderBook()
    orderBook.addQuote("b", 10, 100, 0)
    orderBook.addQuote("a", 20, 200, 0)
    orderBook.addTradeSize(10)

    assert((orderBook.liquidityImbalanceBuy(3) - orderBook.liquidityImbalanceSell(3))/(orderBook.liquidityImbalanceBuy(3) + orderBook.liquidityImbalanceSell(3)) == orderBook.liquidityImbalance(3))

  }

  test("LiquidityImbalanceTakesOnlyFirstQuotes") {
    val orderBook = new OrderBook()
    orderBook.addQuote("b", 10, 100, 0)
    orderBook.addQuote("b", 11.78, 100, 1) // this one is should be filtered
    orderBook.addQuote("a", 20, 200, 0)
    orderBook.addQuote("a", 18.13, 200, 1) // this one is should be filtered
    orderBook.addTradeSize(10)

    assert((orderBook.liquidityImbalanceBuy(3) - orderBook.liquidityImbalanceSell(3))/(orderBook.liquidityImbalanceBuy(3) + orderBook.liquidityImbalanceSell(3)) == orderBook.liquidityImbalance(3))

  }

  test("LiquidityImbalanceSellTakesOnlyFirstQuotes") {
    val orderBook = new OrderBook()
    orderBook.addQuote("a", 23, 200, 0)
    orderBook.addQuote("a", 20, 200, 0)
    orderBook.addQuote("a", 19.33, 200, 1) // this one is should be filtered
    orderBook.addQuote("b", 17.78, 100, 1) // this one is should be filtered
    orderBook.addQuote("b", 16, 100, 0)
    orderBook.addQuote("b", 10, 100, 0)
    orderBook.addTradeSize(10)

    val midpoint = 0.5 * (20 + 16)

    assert(-midpoint + 20.0 * 10.0 / 10.0  == orderBook.liquidityImbalanceSell(1))
    assert(-midpoint + 20.0 * 20.0 / 20.0  == orderBook.liquidityImbalanceSell(2))
    assert(-midpoint + (20.0 * 200.0 + 23.0 * 150.0) / 350.0  == orderBook.liquidityImbalanceSell(35))
  }

  test("LiquidityImbalanceBuyTakesOnlyFirstQuotes") {
    val orderBook = new OrderBook()
    orderBook.addQuote("a", 20, 200, 0)
    orderBook.addQuote("a", 19.33, 200, 1) // this one is should be filtered
    orderBook.addQuote("b", 17.78, 100, 1) // this one is should be filtered
    orderBook.addQuote("b", 16, 100, 0)
    orderBook.addQuote("b", 10, 100, 0)
    orderBook.addTradeSize(10)

    val midpoint = 0.5 * (20 + 16)

    assert(midpoint - 16.0 * 10.0 / 10.0  == orderBook.liquidityImbalanceBuy(1))
    assert(midpoint - 16.0 * 20.0 / 20.0  == orderBook.liquidityImbalanceBuy(2))
    assert(midpoint - (16.0 * 100.0 + 10.0 * 50.0) / 150.0  == orderBook.liquidityImbalanceBuy(15))
  }

  test("BidAskSpreadTakesOnlyFirstQuotes") {
    val orderBook = new OrderBook()
    orderBook.addQuote("a", 20, 5, 0)
    orderBook.addQuote("a", 18, 5, 1) // ignored
    orderBook.addQuote("b", 15, 2, 1) // ignored
    orderBook.addQuote("b", 10, 2, 0)

    assert(orderBook.bidAskSpread() == 10)
  }

  test("MidPointTakesOnlyFirstQuotes") {
    val orderBook = new OrderBook()
    orderBook.addQuote("a", 20, 5, 0)
    orderBook.addQuote("a", 18, 5, 1) // ignored
    orderBook.addQuote("b", 15, 2, 1) // ignored
    orderBook.addQuote("b", 10, 2, 0)

    assert(orderBook.midpoint() == 15)
  }

  test("AddTakesOnlyFirstQuotesPart1") {
    val o1 = new OrderBook()
    o1.addQuote("b", 10, 2, 0)
    o1.addQuote("a", 20, 5, 0)
    o1.addTradeSize(1000)

    val o2 = new OrderBook()
    o2.addQuote("b", 100, 2, 1)
    o2.addQuote("a", 200, 5, 1)
    o2.addTradeSize(2000)

    o1.add(o2)

    o1.bids should contain (new Quote(10, 2, 0))
    o1.asks should contain (new Quote(20, 5, 0))
    o1.bids should not contain (new Quote(100, 2, 1))
    o1.asks should not contain (new Quote(200, 5, 1))
    o1.tradeSizes should contain (1000)
    o1.tradeSizes should contain (2000)
  }

  test("AddTakesOnlyFirstQuotesPart2") {
    val o1 = new OrderBook()
    o1.addQuote("b", 10, 2, 1)
    o1.addQuote("a", 20, 5, 1)
    o1.addTradeSize(1000)

    val o2 = new OrderBook()
    o2.addQuote("b", 100, 2, 0)
    o2.addQuote("a", 200, 5, 0)
    o2.addTradeSize(2000)

    o1.add(o2)

    o1.bids should not contain (new Quote(10, 2, 1))
    o1.asks should not contain (new Quote(20, 5, 1))
    o1.bids should contain (new Quote(100, 2, 0))
    o1.asks should contain (new Quote(200, 5, 0))
    o1.tradeSizes should contain (1000)
    o1.tradeSizes should contain (2000)
  }

}
