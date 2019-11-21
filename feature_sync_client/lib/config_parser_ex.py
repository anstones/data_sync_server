#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    import configparser as ConfigParser
except:
    import ConfigParser


class ConfigParserEx(object):
    """
    配置文件解析类
    """
    def __init__(self, path):
        self.path = path

    def get_config(self, section, key):
        config = ConfigParser.ConfigParser()
        config.read(self.path)
        return config.get(section, key)

    def set_config(self, section, key, value):
        config = ConfigParser.ConfigParser()
        config.read(self.path)
        config.set(section, key, value=value)
        config.write(open(path, "w"))

    def get_all_config(self):
        config = ConfigParser.ConfigParser()
        config.read(self.path)
        return config.sections()
