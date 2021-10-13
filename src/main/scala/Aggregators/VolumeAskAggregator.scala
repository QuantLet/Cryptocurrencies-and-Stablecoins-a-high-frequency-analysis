package Aggregators

import org.apache.spark.sql.Row

object VolumeAskAggregator extends VolumeAggregator {

  override def shouldIncludeVolume(data: Row): Boolean = {
    data.getAs[Boolean]("sell")
  }
}
