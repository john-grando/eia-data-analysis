#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, os, logging
from logging.config import fileConfig

loggers = {}

class MyLogger():
    def __init__(
        self,
        logging_file = 'logging.conf',
        logger_name = 'base_logger',
        log_file_list = ['app',]):
        log_path = os.path.join(
            os.path.dirname(
                os.path.abspath(__file__)),
            "logs")
        if not os.path.isdir(log_path):
            os.makedirs(log_path)
        for f in log_file_list:
            f_name = '{}.log'.format(f)
            if not os.path.isfile(
                os.path.join(
                    log_path,
                    f_name)
                ):
                with open(os.path.join(
                    log_path,
                    f_name),
                    'w'):
                    pass
        #prevent re-calling same logger handlers once initialized
        #also prevent bad logger name from being called
        global loggers
        try:
            fileConfig('logging.conf')
            if not loggers.get(logger_name):
                if logger_name in logging.root.manager.loggerDict.keys():
                    self.logger = logging.getLogger(logger_name)
                else:
                    print('Bad logger name passed {}'.format(logger_name))
                    sys.exit(1)
        except Exception as e:
            print('Logger failed to start {}'.format(logger_name))
            import traceback
            print(traceback.print_exc())
        return
