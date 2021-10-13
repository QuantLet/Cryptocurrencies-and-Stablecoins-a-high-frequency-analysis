package Aggregators

import org.apache.spark.sql.Row

object VolumeTotalAggregator extends VolumeAggregator {

  override def shouldIncludeVolume(data: Row): Boolean = true
}

