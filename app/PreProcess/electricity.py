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
        prog = 'Total Energy Preprocess',
        description = 'Pre-Proccesss Total Energy bulk data from EIA website')
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


 # |-- data: array (nullable = true)
 # |    |-- element: array (containsNull = true)
 # |    |    |-- element: string (containsNull = true)

    #make schema
    # int_fields_l = []
    # str_fields_l = ["copyright", "description", "end", "f", "geography", "iso3166", "lat", "latlon", "lon name", "series_id", "source", "start", "units", "data", "last_updated"]
    # timestamp_fields_l = ["last_updated",]
    # str_fields_schema_l = [StructField(field_name, StringType(), nullable=True) for field_name in str_fields_l]
    # timestamp_schema_l = [StructField(field_name, StringType(), nullable=True) for field_name in timestamp_fields_l]
    # total_schema = StructType(str_fields_schema_l)
    # print(total_schema)
    electricity_df = MySpark\
        .spark\
        .read\
        .json('/EIAElec/ELEC.json')\
        .limit(5000)

    electricity_df.printSchema()

    electricity_df = electricity_df\
        .filter(
            electricity_df["f"] == 'M')\
        .drop("latlon")

    electricity_df.limit(10).toPandas().to_csv('tst.csv')

    #save plans to ExplainFiles directory by default
    # MySpark.explain_to_file(
    #     df = total_energy_df,
    #     description = 'preprocess_electricity',
    #     stamp = '')



MySpark = None

if __name__ == "__main__":
    main()
