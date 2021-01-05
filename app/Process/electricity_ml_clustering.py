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
from app.SparkTools import MyPySpark

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
    Transform pre processed electricity dataframe to ML input
    format for clustering
    """
    parser = build_parser()
    args = parser.parse_args(args)
    #ensure only one sc and spark instance is running
    global MySpark
    MySpark = MySpark or MyPySpark(master = 'local[3]')

    electricity_dim_df = MySpark\
        .spark\
        .read\
        .parquet("/Processed/ElectricityPlantLevelDimDF")\
        .filter(
            (pysF.col("series_id").rlike("^ELEC\.PLANT\.GEN\.")) &
            (pysF.lower(pysF.col("engine_type")) == 'all primemovers') &
            (pysF.col("f") == "M"))\
        .withColumn(
            "fuel_type",
            pysF.regexp_replace(
                pysF.regexp_replace(
                    pysF.col("fuel_type"),
                    "[^a-zA-Z0-9\s]",
                    ""),
                "\s",
                "_"))\
        .filter(pysF.col("iso3166").isNotNull())\
        .withColumn(
            "state",
            pysF.regexp_extract(
                pysF.col("iso3166"),
                r"^USA-([A-Z]+)",
                1))\
        .select("series_id", "state", "fuel_type", "engine_type", "units", "plant_name")\

    electricity_fact_df = MySpark\
        .spark\
        .read\
        .parquet("/Processed/ElectricityFactDF")

    electricity_df = electricity_fact_df.join(
        pysF.broadcast(electricity_dim_df),
        on = "series_id",
        how = "right"
    )

    print(electricity_df.limit(10).toPandas())
    return

if __name__ == "__main__":
    main()
