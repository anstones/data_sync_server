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

        self.server_host = self.config_parser.get_config("MAIN", "server_host")
        self.server_port = int(self.config_parser.get_config("MAIN", "server_port"))
        self.workers_num = int(self.config_parser.get_config("MAIN", "workers_num"))

        self.mysql_host = self.config_parser.get_config("MYSQL", "host")
        self.mysql_port = int(self.config_parser.get_config("MYSQL", "port"))
        self.mysql_user = self.config_parser.get_config("MYSQL", "user")
        self.mysql_password = self.config_parser.get_config("MYSQL", "password")
        self.mysql_db = self.config_parser.get_config("MYSQL", "db")
        self.mysql_use_ssl = int(self.config_parser.get_config("MYSQL", "use_ssl"))


g_conf_parameter = ConfParameter()
