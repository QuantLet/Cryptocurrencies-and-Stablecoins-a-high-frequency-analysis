PYSPARK_PYTHON=python3 spark-submit --class "main.scala.SparkOrderBooks" --master local[4] \
target/scala-2.12/sparktrades_2.12-0.1.jar BTCUSD \
2021-10-10__18-02-35_arbitrage_trades_BTCUSD_2015-01-01_2020-12-31_1s.csv.gz
