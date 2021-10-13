package Aggregators

import org.apache.spark.sql.Row

object PriceAskAggregator extends PriceAggregator {

  override def shouldIncludePrice(data: Row): Boolean = {
    data.getAs[Boolean]("sell")
  }
}
