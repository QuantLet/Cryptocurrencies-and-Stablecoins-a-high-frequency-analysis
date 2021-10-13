PYSPARK_PYTHON=python3 spark-submit --class "main.scala.SparkDailyBreakdown" --master local[4] \
target/scala-2.12/sparktrades_2.12-0.1.jar \
2021-10-10__16-45-07_arbitrage_trades_BTCUSD_2015-01-01_2020-12-31_1s.csv.gz \
2021-10-10__16-59-38_average_daily_price_usdusd_2019-04-01Z_2020-10-31Z.csv.gz \
2021-10-10__16-51-44_average_daily_price_BTCUSD_2015-01-01_2020-12-31.csv.gz \
2021-10-10__16-57-32_complete_minute_sequence_2020-03-11_2020-03-12_1m.csv.gz
