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
            total_energy_raw_df["f"] == 'M')

    #filter and cleanse data
    #separate into dimension and fact tables
    total_energy_dim_df = total_energy_raw_monthly_df\
        .drop(
            "data"
        )\
        .withColumn(
            "last_updated",
            pysF.to_timestamp("last_updated", "yyyy-MM-dd'T'HH:mm:ssXXX")
        )\
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
    #keep data for future processing
    #use parquet because analsis and ML processes
    #will only pull specific columns, so columnar save
    #format is preferred.

    df_l = [
        {
            "df" : total_energy_dim_df,
            "description" : "preprocess_total_energy_dimensions",
            "path" : "/Processed/TotalEnergyDimDF"},
        {
            "df" : total_energy_fact_df,
            "description" : "preprocess_total_energy_facts",
            "path" : "/Processed/TotalEnergyFactDF"}
    ]

    for df in df_l:
        MySpark.eia_output_df(
            df_d = df,
            display_output = args.display_test,
            s3_backup = args.s3
        )

if __name__ == "__main__":
    main()
