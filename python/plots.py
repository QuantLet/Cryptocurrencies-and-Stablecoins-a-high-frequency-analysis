import datetime
import json
import sys

import boto3
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DateType, TimestampType

BUCKET_NAME = 'kaiko-delivery-qfinlab-polimi'


def generate_plot(file, xs, ys, title, pairs, double_size=True):
    date_locator = mdates.AutoDateLocator().get_locator(xs.iloc[0], xs.iloc[-1])
    date_fmt = mdates.ConciseDateFormatter(date_locator)

    fig, ax = plt.subplots()
    if double_size:
        fig.set_size_inches(12.8, 9.6)  # double default of (6.4, 4.8)
    for y, pair in zip(ys, pairs):
        ax.plot(xs, y, label=pair)
    ax.grid()
    ax.legend()
    ax.set_title(title)
    ax.xaxis.set_major_locator(date_locator)
    ax.xaxis.set_major_formatter(date_fmt)
    fig.autofmt_xdate()
    fig.savefig(file, dpi=300)


def sample(x, y, number_samples):
    assert len(x) == len(y)
    step = len(x) // number_samples if number_samples and (number_samples < len(x)) else 1
    return x[::step], y[::step]


@udf(returnType=TimestampType())
def day_of(date):
    return datetime.datetime(date.year, date.month, date.day, 0, 0, 0, 0, date.tzinfo)


def function_for(aggregation):
    aggregate_function = {
        "avg": sf.avg, "max": sf.max, "min": sf.min, "sum": sf.sum, "count": sf.count
    }
    return aggregate_function[aggregation]


def main(data, s3_bucket, xaxis, yaxis, filename, title, samples, aggregation):
    spark = SparkSession.builder.appName("SparkPlot").getOrCreate()

    df = spark.read \
        .option("header", True) \
        .option("inferschema", True) \
        .csv(f"{s3_bucket}data/secondly-arbitrage/{data}/*.gz")

    df.printSchema()

    # collect as pandas df in master node, aggregate (optional) and sample
    if aggregation:
        pdf = df \
            .withColumn("aggregated_x", day_of(col(xaxis))) \
            .groupBy(col("aggregated_x"))\
            .agg(function_for(aggregation)(yaxis).alias("aggregated_y"))\
            .orderBy('aggregated_x')\
            .toPandas()
        print(pdf)
        x_values, y_values = sample(pdf['aggregated_x'], pdf['aggregated_y'], samples)
    else:
        pdf = df \
            .orderBy(xaxis) \
            .toPandas()
        x_values, y_values = sample(pdf[xaxis], pdf[yaxis], samples)

    # create png file
    generate_plot(filename, x_values, [y_values], title, [df.first()['symbol']])

    # upload to s3
    if s3_bucket:
        s3 = boto3.client('s3')
        s3.upload_file(filename, BUCKET_NAME, f'results/figures/{filename}')

    spark.stop()


if __name__ == "__main__":
    parameters = json.loads(sys.argv[1])
    # x_axis is assumed to be a date
    main(parameters['result_directory'],
         parameters['bucket'] if 'bucket' in parameters else "",
         parameters['x_axis'],
         parameters['y_axis'],
         parameters['file_name'],
         parameters['plot_title'],
         parameters['samples'] if 'samples' in parameters else None,
         parameters['aggregation'] if 'aggregation' in parameters else "")
