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
    # int_fields_l = []
    # str_fields_l = []
    # int_fields_schema_l = [StructField(field_name, IntegerType(), nullable=True) for field_name in int_fields_l]
    # total_schema = StructType(
    #     extend(
    #         int_fields_schema_l,
    #         str_fields_schema_l
    #     )
    # )
    #remove .option('inferSchema') and add schema as second argument to .json for explicit structure
    total_energy_df = MySpark\
        .spark\
        .read\
        .option("inferSchema", "true")\
        .json('/EIATotal/TOTAL.json')
    #filter and cleanse data
    #records are comprised of monthly, quarterly, and annual data
    #return only monthly data as the others can be calculated later
    #also remove the 'monthly' indicator in the series name and other
    #extraneous information
    #cleanse remaining columns
    total_energy_df = total_energy_df\
        .filter(
            total_energy_df["f"] == 'M')\
        .withColumn(
            "name",
            pysF.regexp_replace(
                "name",
                "^(.*)(, Monthly)$",
                r"$1"))\
        .withColumn(
            "series_id",
            pysF.regexp_replace(
                "series_id",
                "^[^.]+\.([^.]+)(\..*)*",
                r"$1"))\
        .withColumn(
            "data_exploded",
            pysF.explode("data"))\
        .withColumn(
            "date_raw",
            pysF.col("data_exploded").getItem(0))\
        .withColumn(
            "date",
            pysF.to_date(
                pysF.unix_timestamp(
                    pysF.col("date_raw"),
                    'yyyyMM').cast("timestamp")))\
        .withColumn(
            "value",
            pysF.col("data_exploded").getItem(1))\
        .withColumn(
            "data",
            pysF.concat_ws(", ", pysF.col("data_exploded")))\
        .withColumn(
            "childseries",
            pysF.concat_ws(", ", pysF.col("childseries")))\
        .drop(
            "data_exploded",
            "date_raw")\
        .replace(
            {
                "":None,
                "null":None
            })
    #save plans to ExplainFiles directory by default
    MySpark.explain_to_file(
        df = total_energy_df,
        description = 'total_energy_cleanse',
        stamp = '')

    #keep data for future processing
    total_energy_df.write.csv(
        path = '/Processed/TotalEnergyDF',
        mode = 'overwrite',
        header = True)

    if args.display_test:
        try:
            pd.set_option('display.max_columns', 20)
            MySpark.logger.info("Sample:\n%sDataframe:\n%s",
            total_energy_df.limit(10).toPandas().dtypes,
            total_energy_df.limit(5).toPandas().head())
        finally:
            pd.set_option('display.max_columns', 0)

    if args.s3:
        S3O = S3Access(
            bucket = 'power-plant-data',
            key = 'processed')
        S3O.sync_hdfs_to_s3(
            hdfs_site = 'hdfs://localhost:9000',
            hdfs_folder = 'Processed/TotalEnergyDF')

if __name__ == "__main__":
    main()
