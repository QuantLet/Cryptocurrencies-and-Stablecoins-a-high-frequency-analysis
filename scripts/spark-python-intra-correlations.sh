parameters='{
"result_directory" : "2021-10-10__18-10-14_clean_order_book_and_trades_btcusd_1m.csv.gz",
"interval_minutes" : 1
}'

echo "$parameters"

PYSPARK_PYTHON=python3 spark-submit --master local[4] \
python/intra-pair-correlation.py "$parameters"