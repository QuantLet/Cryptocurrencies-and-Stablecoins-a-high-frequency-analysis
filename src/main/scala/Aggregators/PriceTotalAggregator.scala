package Aggregators

import org.apache.spark.sql.Row

object PriceTotalAggregator extends PriceAggregator {

  override def shouldIncludePrice(data: Row): Boolean = true

}
