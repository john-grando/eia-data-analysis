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
    #filter and cleanse data
    #records are comprised of monthly, quarterly, and annual data
    #return only monthly data as the others can be calculated later
    #also remove the 'monthly' indicator in the series name and other
    #extraneous information
    #cleanse remaining columns
    total_df = total_df\
        .filter(
            total_df["f"] == 'M')\
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
            pysF.col("data_exploded").getItem(1))
    total_df.printSchema()

    #repartition is only necessary if total_df is being used consistently later on.
    #same for cache. repartition number used due to heap space limitation on local machine
    total_df.repartition(
        "series_id")\
        .cache()
    yearly_sum_df = total_df\
        .groupBy(
            "series_id",
            pysF.year("date"))\
        .agg(
            pysF.sum("value").alias("value"),
            pysF.collect_set("name").getItem(0).alias("name"),
            pysF.collect_set("units").getItem(0).alias("units"))\
        .orderBy(pysF.year("date"), "series_id")
    yearly_sum_df.printSchema()
    yearly_sum_df.show()
    #Test to make sure names and units do not vary with series
    # print(yearly_sum_df.select(pysF.size("collect_set(name)")).agg(pysF.max("size(collect_set(name))")).collect()[0])
    # print(yearly_sum_df.select(pysF.size("collect_set(units)")).agg(pysF.max("size(collect_set(units))")).collect()[0])
    # sys.exit()

    #save explain() to file
    MySpark.explain_to_file(
        df = total_df,
        description = 'total_data_cleanse',
        stamp = '')

    MySpark.explain_to_file(
        df = yearly_sum_df,
        description = 'total_yearly_sum',
        stamp = '')

    yearly_sum_df.write.csv(
        path = '/OutputFiles/total_yearly_sum_df',
        mode = 'overwrite',
        header = True)

    S3O = S3Access(
        bucket = 'power-plant-data',
        key = 'output-files'
    )
    S3O.sync_hdfs_to_s3(
        hdfs_site = 'hdfs://localhost:9000',
        hdfs_folder = 'OutputFiles'
    )

if __name__ == "__main__":
    main()
