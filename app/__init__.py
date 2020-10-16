#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, os, logging
from logging.config import fileConfig

loggers = {}

py_file_path = os.path.join(
    os.path.dirname(
        os.path.abspath(__file__),
    ),
    ".."
)

class MyLogger():
    def __init__(
        self,
        logging_file = 'logging.conf',
        logger_name = 'base_logger',
        log_file_list = ['app',]):
        #None may be passed from kwargs arguments in other classes
        #which means a base logger needs to be specified
        logger_name = logger_name or 'base_logger'
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
            if not loggers.get(logger_name):
                if logger_name == 'jupyter':
                    self.logger = logging.getLogger()
                    self.logger.setLevel(logging.INFO)
                elif logger_name == 'jupyter-silent':
                    self.logger = logging.getLogger()
                    self.logger.setLevel(logging.ERROR)
                else:
                    fileConfig(os.path.join(py_file_path,'logging.conf'))
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
