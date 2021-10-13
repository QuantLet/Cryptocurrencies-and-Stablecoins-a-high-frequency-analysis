PYSPARK_PYTHON=python3 spark-submit --class "main.scala.SparkMonthlyArbitrageMeasures" --master local[4] \
target/scala-2.12/sparktrades_2.12-0.1.jar \
ethpax \
paxusd-daily-price \
ethusd-daily-price

#2020-12-14__15-06-02_arbitrage_trades_BTCUSDT_2015-01-01_2020-12-31_1s.csv.gz \
#2020-12-07__19-12-13_average_daily_price_USDTUSD_2015-01-01_2020-12-31.csv.gz \
#2020-12-14__16-00-20_average_daily_price_BTCUSD_2015-01-01_2020-12-31.csv.gz