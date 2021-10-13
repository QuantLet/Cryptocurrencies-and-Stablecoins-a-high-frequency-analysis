parameters='{
"result_directory" : "time-series-test",
"interval_minutes" : 5
}'

echo "$parameters"

PYSPARK_PYTHON=python3 spark-submit --master local[4] \
python/cross-pair-correlation.py "$parameters"