#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys


def get_script_path():
    real_path = os.path.realpath(sys.argv[0])
    real_file = os.path.split(real_path)
    return real_file[0]


def get_log_dir():
    log_dir = os.path.join(get_script_path(), 'log')
    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)
    return os.path.join(get_script_path(), 'log')


def single_ton(cls):
    ins = dict()
    def _warpper(*arg,**kwars):
        if not cls in ins:
            obj = cls(*arg,**kwars)
            ins[cls] = obj
        return ins[cls]
    return _warpper
