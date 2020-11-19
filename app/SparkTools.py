#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
import pyspark.sql.functions as pysF
from pyspark.sql import DataFrame
from typing import Iterable
import pandas as pd

from app import MyLogger
from app.S3Tools import S3Access

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
                    .set('spark.driver.memory', '4g')\
                    .set('spark.executor.memory', '3g')\
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

    @staticmethod
    def melt(
            df: DataFrame,
            id_vars: Iterable[str], value_vars: Iterable[str],
            var_name: str="variable", value_name: str="value") -> DataFrame:
        """
        Convert :class:`DataFrame` from wide to long format.
        Source: https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe
        """

        # -------------------------------------------------------------------------------
        # Create array<struct<variable: str, value: ...>>
        # -------------------------------------------------------------------------------
        _vars_and_vals = pysF.array(*(
            pysF.struct(pysF.lit(c).alias(var_name), pysF.col(c).alias(value_name))
            for c in value_vars))

        # -------------------------------------------------------------------------------
        # Add to the DataFrame and explode
        # -------------------------------------------------------------------------------
        _tmp = df.withColumn("_vars_and_vals", pysF.explode(_vars_and_vals))

        cols = id_vars + [
                pysF.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
        return _tmp.select(*cols)

    def print_df_samples(self, df, df_limit=10, max_columns=20):
        """
        General printout of DataFrames
        """
        pd.set_option('display.max_columns', max_columns)
        self.logger.info("Sample:\n%sDataframe:\n%s\n",
        df.limit(df_limit).toPandas().dtypes,
        df.limit(df_limit).toPandas().head())
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

    def eia_output_df(self, df_d, display_output = None, s3_backup = None):
        """
        Handle dictionary containing dataframe for logging output and file save.
        input dictionary (df_d) should have these keys:
        df - DataFrame
        description - non-whitespace explanation of DataFrame
        path - distributed file system path
        """
        df_key_l = ["df", "description", "path"]
        if not all(k in df_key_l for k in df_d.keys()):
            self.logger.error("bad dictionary passed to output function: %s", df_d)
            return
        self.explain_to_file(
            df = df_d["df"],
            description = df_d["description"],
            stamp = '')
        df_d["df"].write\
            .parquet(
                path = df_d["path"],
                mode = 'overwrite')

        if display_output:
            try:
                pd.set_option('display.max_columns', 20)
                self.logger.info("%s", df_d["description"])
                self.print_df_samples(df = df_d["df"])
            finally:
                pd.set_option('display.max_columns', 0)

        if s3_backup:
            S3O = S3Access(
                bucket = 'power-plant-data',
                key = "backup" + df_d["path"])
            S3O.sync_hdfs_to_s3(
                hdfs_site = 'hdfs://localhost:9000',
                hdfs_folder = df_d["path"])
            self.logger.info("s3 synced for %s", df_d["path"])
        return
