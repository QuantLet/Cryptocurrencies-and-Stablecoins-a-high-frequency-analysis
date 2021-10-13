PYSPARK_PYTHON=python3 spark-submit --class "main.scala.SparkDailyAveragePrice" --master local[4] \
target/scala-2.12/sparktrades_2.12-0.1.jar BTCUSD 2015-01-01 2020-12-31