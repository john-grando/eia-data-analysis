import os, sys, re, argparse
import pandas as pd
from pyspark.sql.types import *
import pyspark.sql.functions as pysF
import pyspark.sql.types as pysT

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
        .json('/EIAElec/ELEC.json', schema = electricity_schema)

    electricity_raw_monthly_df = electricity_raw_df\
        .filter(pysF.col("f") == 'M')

    electricity_fact_df = MyPySpark.eia_data_explode(
        electricity_raw_monthly_df\
            .filter(pysF.col("series_id").isNotNull())\
            .select(
                "series_id",
                "data"))

    electricity_base_dim_df = electricity_raw_monthly_df\
        .drop("data", "latlon")\
        .filter(pysF.col("series_id").isNotNull())\
        .withColumn(
            "last_updated",
            pysF.to_timestamp("last_updated", "yyyy-MM-dd'T'HH:mm:ssXXX"))\
        .withColumn(
            "lat",
            pysF.col("lat").cast(pysT.DoubleType())
        )\
        .withColumn(
            "lon",
            pysF.col("lat").cast(pysT.DoubleType())
        )\
        .withColumn(
            "start",
            pysF.col("start").cast(pysT.IntegerType())
        )\
        .withColumn(
            "end",
            pysF.col("end").cast(pysT.IntegerType())
        )\
        .withColumn(
            "state",
            pysF.regexp_extract(pysF.col("iso3166"), r".*-(.*)$", 1)
        )

    power_columns_l = [
        "Fuel consumption MMBtu",
        "Net generation"
    ]

    receipt_columns_l = [
        "Receipts of fossil fuels by electricity plants",
    ]

    electricity_power_dim_df = electricity_base_dim_df\
        .filter(
            pysF.col("name").rlike("|".join(power_columns_l))
        )\
        .withColumn(
            "split_name",
            pysF.split("name", ":")
        )\
        .withColumn(
            "value_type",
            pysF.trim(pysF.col("split_name").getItem(0))
        )\
        .withColumn(
            "plant_name",
            pysF.trim(pysF.col("split_name").getItem(1))
        )\
        .withColumn(
            "fuel_type",
            pysF.trim(pysF.col("split_name").getItem(2))
        )\
        .withColumn(
            "engine_type",
            pysF.trim(pysF.col("split_name").getItem(3))
        )\
        .withColumn(
            "frequency",
            pysF.trim(pysF.col("split_name").getItem(4))
        )\
        .withColumn(
            "plant_id",
            pysF.regexp_extract(pysF.col("series_id"), r".*\.(\d+)-.*", 1)
        )\
        .drop("split_name")\
        .replace(
            {
                "":None,
                "null":None
            })

    electricity_receipts_dim_df = electricity_base_dim_df\
        .filter(
            pysF.col("name").rlike("|".join(receipt_columns_l))
        )\
        .withColumn(
            "split_name",
            pysF.split("name", ":")
        )\
        .withColumn(
            "value_type",
            pysF.trim(pysF.col("split_name").getItem(0))
        )\
        .withColumn(
            "fuel_type",
            pysF.trim(pysF.col("split_name").getItem(1))
        )\
        .withColumn(
            "plant_type",
            pysF.trim(pysF.col("split_name").getItem(3))
        )\
        .withColumn(
            "frequency",
            pysF.trim(pysF.col("split_name").getItem(4))
        )\
        .drop("split_name")\
        .replace(
            {
                "":None,
                "null":None
            })

    electricity_dim_df = electricity_base_dim_df\
        .filter(
            ~pysF.col("name").rlike(
                "|".join(
                    power_columns_l +
                    receipt_columns_l)
            )
        )\
        .withColumn(
            "split_name",
            pysF.split("name", ":")
        )\
        .drop("split_name")\
        .replace(
            {
                "":None,
                "null":None
            })

    # save plans to ExplainFiles, write to hdfs, and sync

    df_l = [
        {
            "df" : electricity_fact_df,
            "description" : "preprocess_electricity_facts",
            "path" : "/Processed/ElectricityFactDF"},
        {
            "df" : electricity_power_dim_df,
            "description" : "preprocess_electricity_power_dimensions",
            "path" : "/Processed/ElectricityPowerDimDF"},
        {
            "df" : electricity_dim_df,
            "description" : "preprocess_electricity_dimensions",
            "path" : "/Processed/ElectricityDimDF"},
        {
            "df" : electricity_receipts_dim_df,
            "description" : "preprocess_electricity_receipts_dimensions",
            "path" : "/Processed/ElectricityReceiptsDimDF"},
    ]

    for df in df_l:
        MySpark.eia_output_df(
            df_d = df,
            display_output = args.display_test,
            s3_backup = args.s3
        )

if __name__ == "__main__":
    main()
