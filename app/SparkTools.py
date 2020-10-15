#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from app import MyLogger

class MyPySpark():

    def __init__(self, master = 'local[*]'):
        self.logger = MyLogger().logger
        try:
            self.sc and self.spark
        except (AttributeError, NameError, UnboundLocalError) as e:
            try:
                self.logger.info('sc and spark not found: %s', e)
                import findspark
                findspark.init()
                import pyspark
                conf = pyspark\
                    .SparkConf()\
                    .setAppName("PowerPlant")\
                    .setMaster(master)
                self.sc = pyspark.SparkContext(conf=conf)
                self.sc.setLogLevel('WARN')
                self.spark = pyspark\
                    .sql\
                    .SparkSession(self.sc)\
                    .builder\
                    .getOrCreate()
            except Exception as e:
                self.logger.error("pyspark failed to initialize")
                self.logger.exception(e)
        return

    @staticmethod
    def explain_to_file(
        df,
        loc = 'ExplainFiles',
        description = '',
        stamp = datetime.datetime.now(),
        logger_output = False):
        try:
            if logger_output:
                logger_output.info(df._jdf.queryExecution().toString())
            with open('/'.join([loc,'spark_explain_{}.{}.txt'.format(description, stamp or 'general')]), 'w') as f:
                f.write(df._jdf.queryExecution().toString())
        except:
            print('explain plan output failed')
        return
