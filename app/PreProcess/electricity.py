import os, sys, re, argparse
import pandas as pd
from pyspark.sql.types import *
import pyspark.sql.functions as pysF

py_file_path = os.path.join(
    os.path.dirname(
        os.path.abspath(__file__),
    ),
    "..",
    ".."
)
sys.path.append(py_file_path)
from app import MyLogger
from app.SparkTools import MyPySpark
from app.S3Tools import S3Access

def build_parser():
    """
    Build argument parser.
    """
    parser = argparse.ArgumentParser(
        prog = 'Plant Electricity Preprocess',
        description = 'Pre-Proccesss plant electricity bulk data from EIA website')
    parser.add_argument(
        '--display-test',
        '-t',
        action = 'store_true',
        help = 'Display sample output')
    parser.add_argument(
        '--s3',
        '-s3',
        action = 'store_true',
        help = 'Backup output to s3')
    return parser

MySpark = None

def main(args = None):
    """
    Pre process raw input data and save in
    cleansed state to /Processed directory
    """
    parser = build_parser()
    args = parser.parse_args(args)
    #ensure only one sc and spark instance is running
    global MySpark
    MySpark = MySpark or MyPySpark(master = 'local[3]')

    #make schema
    int_fields_l = []
    str_fields_l = [
        "copyright",
        "description",
        "end",
        "f",
        "geography",
        "iso3166",
        "lat",
        "latlon",
        "lon",
        "name",
        "source",
        "start",
        "units",
        "last_updated"]
    str_fields_no_null_l = [
        "series_id",
    ]
    # timestamp_fields_l = ["last_updated",]
    str_fields_schema_l = [
        StructField(
            field_name,
            StringType(),
            nullable=True
        ) for field_name in str_fields_l]
    str_fields_no_null_schema_l = [
        StructField(
            field_name,
            StringType(),
            nullable=False
        ) for field_name in str_fields_no_null_l]
    array_fields_l = [
        StructField(
            "data",
            ArrayType(
                ArrayType(
                    StringType()
                )
            )
        ),
    ]
    # timestamp_schema_l = [StructField(field_name, StringType(), nullable=True) for field_name in timestamp_fields_l]
    electricity_schema = StructType(
        str_fields_schema_l +
        str_fields_no_null_schema_l +
        array_fields_l)
    #limit for testing
    electricity_raw_df = MySpark\
        .spark\
        .read\
        .json('/EIAElec/ELEC.json', schema = electricity_schema)\
        .limit(2000)

    electricity_raw_monthly_df = electricity_raw_df\
        .filter(
            pysF.col("f") == 'M')\
        .drop("latlon")

    electricity_fact_df = electricity_raw_monthly_df\
        .select(
            "series_id",
            "data")

    electricity_dim_df = electricity_raw_monthly_df\
        .drop("data")

    print('facts')
    electricity_fact_df.printSchema()
    electricity_fact_df.show()
    print("dimensions")
    electricity_dim_df.printSchema()
    electricity_dim_df.show()


    # save plans to ExplainFiles directory by default
    MySpark.explain_to_file(
        df = electricity_dim_df,
        description = 'preprocess_electricity_dimensions',
        stamp = '')

    MySpark.explain_to_file(
        df = electricity_fact_df,
        description = 'preprocess_electricity_facts',
        stamp = '')

    electricity_dim_df.write\
        .parquet(
            path = '/Processed/ElectricityDimDF',
            mode = 'overwrite')

    electricity_fact_df.write\
        .parquet(
            path = '/Processed/ElectricityFactDF',
            mode = 'overwrite')

    if args.display_test:
        try:
            pd.set_option('display.max_columns', 20)
            MySpark.logger.info("Dimension Table")
            MySpark.logger.info("Sample:\n%sDataframe:\n%s",
            electricity_dim_df.limit(10).toPandas().dtypes,
            electricity_dim_df.limit(5).toPandas().head())
            MySpark.logger.info("Fact Table")
            MySpark.logger.info("Sample:\n%sDataframe:\n%s",
            electricity_fact_df.limit(10).toPandas().dtypes,
            electricity_fact_df.limit(5).toPandas().head())
        finally:
            pd.set_option('display.max_columns', 0)

    if args.s3:
        S3O = S3Access(
            bucket = 'power-plant-data',
            key = 'processed')
        S3O.sync_hdfs_to_s3(
            hdfs_site = 'hdfs://localhost:9000',
            hdfs_folder = 'Processed/ElectricityDimDF')
        S3O.sync_hdfs_to_s3(
            hdfs_site = 'hdfs://localhost:9000',
            hdfs_folder = 'Processed/ElectricityFactDF')

if __name__ == "__main__":
    main()
