#!/home/grandocu/anaconda3/envs/colt/bin
# -*- coding: utf-8 -*-

import os, sys, re, tempfile
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
                'No files were returned from s3 bucket: %s and location: %s',
                self.bucket,
                self.key)
            sys.exit(1)
        return s3Contents
