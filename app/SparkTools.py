#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from app import MyLogger
from pyspark.sql import DataFrame
from pyspark.rdd import RDD

class MyPySpark():

    def __init__(self, master = 'local[*]', **kwargs):
        self.logger = MyLogger(logger_name = kwargs.get('logger_name')).logger
        try:
            self.sc and self.spark
        except (AttributeError, NameError, UnboundLocalError) as e:
            try:
                self.logger.info('sc and spark not found: %s', e)
                import findspark
                findspark.init()
                import pyspark
                #set spark.driver.memory instead of executor when running in standalone
                conf = pyspark\
                    .SparkConf()\
                    .set('spark.driver.memory', '6g')\
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
        location = 'ExplainFiles',
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
