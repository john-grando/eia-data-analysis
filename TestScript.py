#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 22 14:48:09 2020

@author: grandocu
"""

class PySparkClass():

    def __init__(self):
        try:
            self.sc and self.spark
        except (AttributeError, NameError, UnboundLocalError) as e:
            print('sc and spark not found: {}'.format(e))
            import findspark
            findspark.init()
            import pyspark
            conf = pyspark\
                .SparkConf()
            self.sc = pyspark.SparkContext(conf=conf)
            self.sc.setLogLevel('WARN')
            self.spark = pyspark\
                .sql\
                .SparkSession(self.sc)\
                .builder\
                .appName("testingAppName")\
                .getOrCreate()
            return

def explain_to_file(loc, ext, df):
    with open('spark_explain_{}.txt'.format(ext), 'w') as f:
        f.write(df._jdf.queryExecution().toString())
    return

import os, sys, random, re
from pyspark.sql.types import *

int_fields_l = [
    'STATION',
    'WBAN']

int_fields_schema_l = [StructField(field_name, IntegerType(), nullable=True) for field_name in int_fields_l]

weather_schema = StructType(int_fields_schema_l)

file_path = os.path.join(os.path.dirname(__file__), "WeatherCSV")
only_csv_rgx = re.compile(r'.*\.csv$')
file_list = [f for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path, f)) and re.match(only_csv_rgx, f)]


random.seed(82828)
file_list_short = ['hdfs://localhost:9000/WeatherCSV/{}'.format(f) for f in random.sample(file_list, 200)]

myClass = PySparkClass()
# .schema(weather_schema)\
df_load = myClass\
    .spark\
    .read\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .csv(file_list_short)

df_load.printSchema()
df_load\
    .limit(10)\
    .coalesce(1)\
    .write\
    .mode('overwrite')\
    .csv(
        'initial_tst.csv',
        header=True)

df_1 = df_load.select(['STATION', 'DATE', 'TEMP'])
print(df_1.take(3))

df_agg = df_1.groupby('STATION').agg({'TEMP':'mean'}).withColumnRenamed('avg(TEMP)', 'AVG_TEMP')
print(df_agg.explain(extended=True))
explain_to_file(loc = './', ext = 'df_agg', df = df_agg)
print(df_agg.take(5))
