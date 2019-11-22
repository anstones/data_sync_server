#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import logging.handlers


class BaseLogger(object):
    def __init__(self):
        self._basic_logger = logging.getLogger(__name__)

        self._log_format = logging.Formatter(fmt="[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s")

        self._stream_handler = logging.StreamHandler()
        self._stream_handler.setFormatter(self._log_format)

        self._basic_logger.addHandler(self._stream_handler)
        self._basic_logger.setLevel(logging.NOTSET)

        self._logger = self._basic_logger

    def __getattr__(self, item):
        return getattr(self._logger, item)


class DailyLogger(BaseLogger):
    def __init__(self):
        super().__init__()
        self._file_handler = None
        self._child_name = "daily_logger"
        self._file_path = "/tmp"
        self._file_name = "daily_logger.log"
        self._back_count = 10
        self._logger_level = logging.NOTSET

    def set_child_name(self, child_name):
        self._child_name = child_name

    def set_file_path(self, file_path):
        if not os.path.isdir(file_path):
            os.mkdir(file_path)
        self._file_path = file_path

    def set_file_name(self, file_name):
        self._file_name = file_name

    def set_back_count(self, back_count):
        self._back_count = back_count

    def set_log_level(self, level):
        temp_level_dict = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warn": logging.WARN,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "fatal": logging.FATAL,
            "critical": logging.CRITICAL
        }

        self._logger_level = temp_level_dict.get(level.lower(), logging.NOTSET)

    def start(self):
        self._logger = self._basic_logger.getChild(self._child_name)

        self._file_handler = logging.handlers.TimedRotatingFileHandler(
                                                    filename=os.path.join(self._file_path, self._file_name),
                                                    when='D',
                                                    interval=1,
                                                    backupCount=self._back_count)
        self._file_handler.setFormatter(self._log_format)

        self._logger.addHandler(self._file_handler)
        self._logger.setLevel(self._logger_level)


logger = DailyLogger()


if __name__ == '__main__':
    logger.set_child_name('test')
    logger.set_file_path("/tmp")
    logger.set_file_name("test.log")
    logger.set_log_level('DEBUG')
    logger.start()

    logger.info("test {}".format("test_1234"))
