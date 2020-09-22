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
            self.spark = pyspark\
                .sql\
                .SparkSession(self.sc)\
                .builder\
                .appName("testingAppName")\
                .getOrCreate()
            return

import os, sys, random, re
file_path = os.path.join(os.path.dirname(__file__), "WeatherCSV")
only_csv_rgx = re.compile(r'.*\.csv$')
file_list = [f for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path, f)) and re.match(only_csv_rgx, f)]

random.seed(82828)
file_list_short = ['hdfs://localhost:9000/WeatherCSV/{}'.format(f) for f in random.sample(file_list, 200)]

myClass = PySparkClass()
df_load = myClass\
    .spark\
    .read\
    .option("header", "true")\
    .csv(file_list_short)

print(df_load.take(2))

df_1 = df_load.select(['STATION', 'DATE', 'TEMP'])
print(df_1.take(3))

df_agg = df_1.groupby('STATION').agg({'TEMP':'mean'}).withColumnRenamed('avg(TEMP)', 'AVG_TEMP')
print(df_agg.explain(extended=True))
print(df_agg.take(5))