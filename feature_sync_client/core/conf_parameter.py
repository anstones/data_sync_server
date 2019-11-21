#!/usr/bin/env python
# -*- coding: utf-8 -*-

from lib.config_parser_ex import ConfigParserEx
from lib.utils import single_ton


@single_ton
class ConfParameter(object):
    def load_conf(self, path):
        self.config_parser = ConfigParserEx(path)

        self.log_level = self.config_parser.get_config("LOG", "level")
        self.log_file_name = self.config_parser.get_config("LOG", "file_name")
        self.log_back_count = int(self.config_parser.get_config("LOG", "back_count"))

        self.sync_time_interval = int(self.config_parser.get_config("MAIN", "interval"))
        self.sync_run_once = int(self.config_parser.get_config("MAIN", "run_once"))
        self.sync_province_code = self.config_parser.get_config("MAIN", "province_code")
        self.sync_city_code = self.config_parser.get_config("MAIN", "city_code")
        _sync_town_code = self.config_parser.get_config("MAIN", "town_code")
        self.sync_town_code = _sync_town_code if _sync_town_code else '-1'

        self.mysql_host = self.config_parser.get_config("MYSQL", "host")
        self.mysql_port = int(self.config_parser.get_config("MYSQL", "port"))
        self.mysql_user = self.config_parser.get_config("MYSQL", "user")
        self.mysql_password = self.config_parser.get_config("MYSQL", "password")
        self.mysql_db = self.config_parser.get_config("MYSQL", "db")

        self.remote_feature_url = self.config_parser.get_config("REMOTE_FEATURE_SERVER", "url")


g_conf_parameter = ConfParameter()
