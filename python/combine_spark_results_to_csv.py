import argparse

from pyspark.sql import SparkSession


def main(input_dir, output_file, flag_monthly, flag_compressed):
    spark = SparkSession.builder.appName("SparkCombineMonthlyArbitrage").getOrCreate()

    if flag_monthly:
        df_monthly_measures = read_df(input_dir, "*monthly_arbitrage_measures*", spark)
        df_monthly_volatility = read_df(input_dir, "*monthly_arbitrage_volatility*", spark)
        df = df_monthly_measures.join(df_monthly_volatility, on=["year_month", "symbol"], how="left")
    elif flag_compressed:
        df = read_df(input_dir, "*", spark, "gz")
    else:
        df = read_df(input_dir, "*", spark)

    df.printSchema()

    write_to_csv(df, output_file)

    spark.stop()


def read_df(input_dir, pattern, spark, suffix="csv"):
    df = spark.read \
        .option("header", True) \
        .option("inferschema", True) \
        .csv(f"{input_dir}/{pattern}/*.{suffix}")
    return df


def write_to_csv(df, output_file):
    pdf = df \
        .toPandas()
    pdf.to_csv(f"{output_file}", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_dir')
    parser.add_argument('output_file')
    parser.add_argument('--monthly', dest='monthly', action='store_true')
    parser.add_argument('--compressed', dest='compressed', action='store_true')
    parser.set_defaults(monthly=False)
    parser.set_defaults(compressed=False)

    args = parser.parse_args()

    main(args.input_dir, args.output_file, args.monthly, args.compressed)
