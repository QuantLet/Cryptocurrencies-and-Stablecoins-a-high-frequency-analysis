PYSPARK_PYTHON=python3 spark-submit --class "main.scala.SparkCorrelations" --master local[4] \
target/scala-2.12/sparktrades_2.12-0.1.jar time-series-test
