PYSPARK_PYTHON=python3 spark-submit --class "main.scala.SparkOBStatistics" --master local[4] \
target/scala-2.12/sparktrades_2.12-0.1.jar \
 2021-10-10__18-03-45_order_book_and_trades_BTCUSD_1m.csv.gz \
 10 \
 false \
 2021-10-10__18-09-23_average_daily_price_BTCUSD_2015-01-01_2020-12-31.csv.gz