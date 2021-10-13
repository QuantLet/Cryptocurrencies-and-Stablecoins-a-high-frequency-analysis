PYSPARK_PYTHON=python3 spark-submit --class "main.scala.SparkGenerateMinutesSequence" --master local[4] \
target/scala-2.12/sparktrades_2.12-0.1.jar 2020-03-11 2020-03-12 1