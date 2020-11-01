#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from app import MyLogger
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
import pyspark.sql.functions as pysF

class MyPySpark(MyLogger):
    """
    General functions for pyspark operations
    """
    def __init__(self, master = 'local[*]', **kwargs):
        super().__init__(logger_name = kwargs.get('logger_name'))
        try:
            self.sc and self.spark
        except (AttributeError, NameError, UnboundLocalError) as e:
            try:
                self.logger.info('sc and spark not found: %s', e)
                import findspark
                findspark.init()
                import pyspark
                #set spark.driver.memory as well as spark.executor.memory when running in standalone
                conf = pyspark\
                    .SparkConf()\
                    .set('spark.driver.memory', '6g')\
                    .set('spark.executor.memory', '5g')\
                    .setAppName("PowerPlant")\
                    .setMaster(master)
                self.sc = pyspark\
                    .SparkContext(conf=conf)
                self.sc.setLogLevel('WARN')
                self.spark = pyspark\
                    .sql\
                    .SparkSession(self.sc)\
                    .builder\
                    .getOrCreate()
                self.logger.info('spark and sc created: %s', self.spark.sparkContext.getConf().getAll())
            except Exception as e:
                self.logger.error("pyspark failed to initialize")
                self.logger.exception(e)
        return

    @staticmethod
    def explain_to_file(
        df,
        location = 'app/ExplainFiles',
        description = '',
        stamp = datetime.datetime.now(),
        logger_output = False):
        try:
            if logger_output:
                logger_output.info(df._jdf.queryExecution().toString())
            with open('/'.join([location,'spark_explain_{}.{}.txt'.format(description, stamp or 'general')]), 'w') as f:
                f.write(df._jdf.queryExecution().toString())
        except:
            print('explain plan output failed')
        return

    def eia_data_explode(df):
        """
        Reformat eia data array into one row per measurement.
        Format should be ["series_id", "data"]
        """
        return df\
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
        .drop(
            "data",
            "data_exploded",
            "date_raw")\
        .replace(
            {
                "":None,
                "null":None
            })
