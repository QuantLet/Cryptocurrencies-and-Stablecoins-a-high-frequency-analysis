package Aggregators

import org.apache.spark.sql.Row

object VolumeBidAggregator extends VolumeAggregator{

  override def shouldIncludeVolume(data: Row): Boolean = {
    val sell = data.getAs[Boolean]("sell")
    !sell
  }
}
