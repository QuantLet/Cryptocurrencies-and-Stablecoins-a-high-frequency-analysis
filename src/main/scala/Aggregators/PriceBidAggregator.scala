package Aggregators

import org.apache.spark.sql.Row

object PriceBidAggregator extends PriceAggregator {

  override def shouldIncludePrice(data: Row): Boolean = {
    val sell = data.getAs[Boolean]("sell")
    !sell
  }
}
