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
    str_fields_l = [
        "category_id",
        "f",
        "name",
        "notes",
        "parent_category_id",
        "units",
        "end",
        "start",
        "last_updated"
    ]
    str_fields_no_null_l = [
        "series_id"
    ]
    array_fields_l = [
        StructField(
            "data",
            ArrayType(
                ArrayType(
                    StringType()
                )
            )
        ),
        StructField(
            "childseries",
            ArrayType(
                StringType()
            )
        )
    ]
    str_fields_schema_l = [
        StructField(
            field_name,
            StringType(),
            nullable=True
        ) for field_name in str_fields_l
    ]
    str_fields_no_null_schema_l = [
        StructField(
            field_name,
            StringType(),
            nullable=False
        ) for field_name in str_fields_no_null_l]
    #int_fields_schema_l = [StructField(field_name, IntegerType(), nullable=True) for field_name in int_fields_l]
    total_schema = StructType(
        str_fields_schema_l +
        str_fields_no_null_schema_l +
        array_fields_l
    )
    #remove .option('inferSchema') and add schema as second argument to .json for explicit structure
    #records are comprised of monthly, quarterly, and annual data
    #return only monthly data as the others can be calculated later
    #and it will save space since most of the operations here are
    #done on a local cluster to save on cost.
    total_energy_raw_df = MySpark\
        .spark\
        .read\
        .option("inferSchema", "true")\
        .json('/EIATotal/TOTAL.json', schema = total_schema)

    total_energy_raw_monthly_df = total_energy_raw_df\
        .filter(
            total_energy_raw_df["f"] == 'M')\

    #filter and cleanse data
    #separate into dimension and fact tables
    total_energy_dim_df = total_energy_raw_monthly_df\
        .drop(
            "data"
        )\
        .withColumn(
            "last_updated",
            pysF.to_utc_timestamp(
                pysF.to_timestamp("last_updated"),
                pysF.regexp_extract(pysF.col("last_updated"), '.*((\+|-)[\d:]+)', 1)))\
        .replace(
            {
                "":None,
                "null":None
            })
    total_energy_fact_df = MyPySpark.eia_data_explode(
        total_energy_raw_monthly_df\
            .select(
                "series_id",
                "data"))
    #save plans to ExplainFiles directory by default
    MySpark.explain_to_file(
        df = total_energy_dim_df,
        description = 'preprocess_total_energy_dimensions',
        stamp = '')

    MySpark.explain_to_file(
        df = total_energy_fact_df,
        description = 'preprocess_total_energy_facts',
        stamp = '')

    #keep data for future processing
    #use parquet because analsis and ML processes
    #will only pull specific columns, so columnar save
    #format is preferred.
    total_energy_dim_df.write\
        .parquet(
            path = '/Processed/TotalEnergyDimDF',
            mode = 'overwrite')

    total_energy_fact_df.write\
        .parquet(
            path = '/Processed/TotalEnergyFactDF',
            mode = 'overwrite')

    if args.display_test:
        try:
            MySpark.logger.info("Dimension Table")
            MySpark.print_df_samples(df = total_energy_dim_df, logger = MySpark.logger)
            MySpark.logger.info("Fact Table")
            MySpark.print_df_samples(df = total_energy_fact_df, logger = MySpark.logger)
        finally:
            pd.set_option('display.max_columns', 0)

    if args.s3:
        S3O = S3Access(
            bucket = 'power-plant-data',
            key = 'processed')
        S3O.sync_hdfs_to_s3(
            hdfs_site = 'hdfs://localhost:9000',
            hdfs_folder = 'Processed/TotalEnergyDimDF')
        S3O.sync_hdfs_to_s3(
            hdfs_site = 'hdfs://localhost:9000',
            hdfs_folder = 'Processed/TotalEnergyFactDF')

if __name__ == "__main__":
    main()
