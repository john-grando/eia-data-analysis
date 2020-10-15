import os, sys, random, re
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

MySpark = None

def main():
    #ensure only one sc and spark instance is running
    global MySpark
    MySpark = MySpark or MyPySpark(master = 'local[3]')
    S3Object = S3Access(
        bucket = 'power-plant-data',
        key = 'eia-total-dataframes'
    )
    S3Object.upload_datafame_s3(
        df = MySpark.sc.parallelize(range(1000)),
        file_name = 'yearly_sum_df.pkl')
    sys.exit()
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
    total_df = MySpark\
        .spark\
        .read\
        .option("inferSchema", "true")\
        .json('/EIATotal/TOTAL.json')
    #records are comprised of monthly, quarterly, and annual data
    #return only monthly data as the others can be calculated later
    #also remove the 'monthly' indicator in the series name
    total_df = total_df\
        .filter(total_df["f"] == 'M')\
        .withColumn(
            "name",
            pysF.regexp_replace(
                "name",
                "^(.*)(, Monthly)$",
                r"$1"))
    #cleanse data
    #remove series_id extraneous info
    #reformat data into columns
    total_df = total_df\
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
            "date",
            pysF.col("data_exploded").getItem(0))\
        .withColumn(
            "date_split",
            pysF.split(
                pysF.regexp_replace(
                    "date",
                    "([0-9]{4})(?!$)",
                    r"$1, "),
                ", "))\
        .withColumn(
            "value",
            pysF.col("data_exploded").getItem(1))\
        .withColumn(
            "year",
            pysF.col("date_split").getItem(0))\
        .withColumn(
            "month",
            pysF.col("date_split").getItem(0))
    total_df.printSchema()
    #partitionBy is only necessary if total_df is being used consistently later on.
    # total_df.write.partitionBy("year", "series_id")
    yearly_sum_df = total_df\
        .groupBy("year", "series_id")\
        .agg(
            pysF.sum("value").alias("value"),
            pysF.collect_set("name").getItem(0).alias("name"),
            pysF.collect_set("units").getItem(0).alias("units"))\
        .orderBy("year", "series_id")
    yearly_sum_df.printSchema()
    #Test to make sure names and units do not vary with series
    # print(yearly_sum_df.select(pysF.size("collect_set(name)")).agg(pysF.max("size(collect_set(name))")).collect()[0])
    # print(yearly_sum_df.select(pysF.size("collect_set(units)")).agg(pysF.max("size(collect_set(units))")).collect()[0])
    # sys.exit()

    #save explain() to file
    MySpark.explain_to_file(
        df = total_df,
        description = 'data_cleanse',
        stamp = '')

    MySpark.explain_to_file(
        df = yearly_sum_df,
        description = 'yearly_sum',
        stamp = '')

if __name__ == "__main__":
    main()
