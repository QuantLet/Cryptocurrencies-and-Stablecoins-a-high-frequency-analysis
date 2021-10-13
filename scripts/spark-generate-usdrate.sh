PYSPARK_PYTHON=python3 spark-submit --class "main.scala.SparkParityUsdRate" --master local[4] \
target/scala-2.12/sparktrades_2.12-0.1.jar 2019-04-01 2020-10-31