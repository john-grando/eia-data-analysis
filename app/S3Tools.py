#!/home/grandocu/anaconda3/envs/colt/bin
# -*- coding: utf-8 -*-

import os, sys, re, tempfile, subprocess
import boto3
from botocore.client import ClientError
from app import MyLogger

class S3Access():
    """
    Class to handle s3 access functions and calls
    """
    def __init__(self, bucket, key):
        self.logger = MyLogger().logger
        #instantiate aws
        self.s3_r = boto3.resource('s3')
        self.s3 = boto3.client('s3')
        self.bucket = bucket
        self.key = key
        return

    def sync_hdfs_to_s3(
        self,
        hdfs_site,
        hdfs_folder):
        """
        sync hdfs folder with s3

        Need to delete all files in key before uploading to
        ensure only new data exists
        """
        self.logger.info('hdfs sync started')
        sub_process = subprocess.Popen(
            [
                "hadoop",
                "distcp",
                "-Dfs.s3a.access.key={}".format(os.getenv('AWS_ACCESS_KEY_ID')),
                "-Dfs.s3a.secret.key={}".format(os.getenv('AWS_SECRET_ACCESS_KEY')),
                "-overwrite",
                "{}/{}/".format(hdfs_site, hdfs_folder),
                "s3a://{}/{}/".format(self.bucket, self.key)
            ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        process_error, _ = sub_process.communicate()
        if sub_process.returncode != 0:
            self.logger.error('hdfs sync failed:\n%s', process_error)
            return
        self.logger.info('hdfs sync ended')
        return

    def get_file_list(
    self,
    file_regex = r'.*'):
        """
        Return a list of files in a s3 bucket with regular expression filter option
        """
        s3Contents = []
        #Use list_objects_v2 via kwargs since there could be
        #more than 1000 objects (single return limit)
        kwargs = {'Bucket': self.bucket, 'Prefix':self.key}
        while True:
            try:
                resp = self.s3.list_objects_v2(**kwargs)
            except:
                resp = None
                self.logger.error('Unable to reach s3 bucket')
                sys.exit(1)
            try:
                f_regex = re.compile(file_regex)
                #python 3.8+ required for walrus operator
                s3Contents = [f['Key'] for f in resp['Contents'] if (match := re.search(f_regex, f['Key']))]
            except Exception as e:
                self.logger.exception(e)
                self.logger.error('failed to filter s3 folder.  Bucket: %s and location: %s',
                    self.bucket,
                    self.key)
                sys.exit(1)
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
        if not s3Contents:
            self.logger.error(
                'No files were returned from s3 bucket: %s and location: %s filtering by %s',
                self.bucket,
                self.key,
                file_regex)
            sys.exit(1)
        return s3Contents
