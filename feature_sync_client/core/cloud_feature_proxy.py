#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64

from lib.utils import single_ton
from lib.mysql_client import MysqlClient
from lib.logger import logger

from core.conf_parameter import g_conf_parameter


class CloudFeatureProxy(object):
    stmt_get_max_user_id = "select MAX(`id`) as id from user where province_code=%(province_code)s and city_code=%(city_code)s \
                            and (%(town_code)s='-1' or town_code=%(town_code)s)"
    stmt_query_user_id_range = "select id from user \
                            where province_code=%(province_code)s and city_code=%(city_code)s \
                            and (%(town_code)s='-1' or town_code=%(town_code)s) \
                            and id>%(begin_id)s and id<=%(end_id)s LIMIT 1000"
    stmt_query_user_range = "select * from user \
                            where province_code=%(province_code)s and city_code=%(city_code)s \
                            and (%(town_code)s='-1' or town_code=%(town_code)s) \
                            and id>%(begin_id)s and id<=%(end_id)s LIMIT 1000"

    stmt_get_max_feature_model_0330_id = "select MAX(`id`) as id from feature_model_0330 \
                                            where province_code=%(province_code)s and city_code=%(city_code)s \
                                            and (%(town_code)s='-1' or town_code=%(town_code)s)"
    stmt_query_feature_model_0330_id_range = "select id from feature_model_0330 \
                                            where province_code=%(province_code)s and city_code=%(city_code)s \
                                            and (%(town_code)s='-1' or town_code=%(town_code)s) \
                                            and id>%(begin_id)s and id<=%(end_id)s LIMIT 1000"
    stmt_query_feature_model_0330_range = "select * from feature_model_0330 \
                                            where province_code=%(province_code)s and city_code=%(city_code)s \
                                            and (%(town_code)s='-1' or town_code=%(town_code)s) \
                                            and id>%(begin_id)s and id<=%(end_id)s LIMIT 1000"
    
    def __init__(self):
        self._database_client = None

    async def _initial_database_client(self):
        """
        初始化数据库连接
        """
        self._database_client = MysqlClient()
        self._database_client.initialize(
                                    host=g_conf_parameter.mysql_host,
                                    port=g_conf_parameter.mysql_port,
                                    user=g_conf_parameter.mysql_user,
                                    password=g_conf_parameter.mysql_password,
                                    db=g_conf_parameter.mysql_db)
        await self._database_client.create_pool()

    async def _get_database_client(self):
        if self._database_client is None:
            await self._initial_database_client()

    async def get_sync_status(self, province_code, city_code, town_code):
        """
        获取同步状态
        :return:
        """
        await self._get_database_client()
            
        max_feature_model_0330_id = await self._database_client.query_one(self.stmt_get_max_feature_model_0330_id,
                                                                            parameter=dict(
                                                                                    province_code=province_code,
                                                                                    city_code=city_code,
                                                                                    town_code=town_code))
        max_feature_model_0330_id = max_feature_model_0330_id["id"]
        if max_feature_model_0330_id is None:
            max_feature_model_0330_id = -1

        max_user_id = await self._database_client.query_one(self.stmt_get_max_user_id,
                                                            parameter=dict(
                                                                    province_code=province_code,
                                                                    city_code=city_code,
                                                                    town_code=town_code))
        max_user_id = max_user_id["id"]
        if max_user_id is None:
            max_user_id = -1

        return {
            'user_id': max_user_id,
            'feature_model_0330_id': max_feature_model_0330_id
        }

    async def query_user_id_range(self, province_code, city_code, town_code, begin_id, end_id):
        return await self._query_info_by_id_range(self.stmt_query_user_id_range, province_code, city_code, town_code, begin_id, end_id)

    async def query_user_range(self, province_code, city_code, town_code, begin_id, end_id):
        content = await self._query_info_by_id_range(self.stmt_query_user_range, province_code, city_code, town_code, begin_id, end_id)

        # content 值 ((7026, '42489ff329f4456a8f241df79de797f1', b'python2的pickle编码的二进制', '510000', '511102', '511100'),)
        for c in content:
            if isinstance(c["pic_md5"], bytes):
                c["pic_md5"] = base64.b64encode(c["pic_md5"]).decode()
            
        return content

    async def query_feature_model_0330_id_range(self, province_code, city_code, town_code, begin_id, end_id):
        return await self._query_info_by_id_range(self.stmt_query_feature_model_0330_id_range, province_code, city_code, town_code, begin_id, end_id)

    async def query_feature_model_0330_range(self, province_code, city_code, town_code, begin_id, end_id):
        content = await self._query_info_by_id_range(self.stmt_query_feature_model_0330_range, province_code, city_code, town_code, begin_id, end_id)
        for c in content:
            if isinstance(c["feature"], bytes):
                c["feature"] = c["feature"].decode()

        return content

    async def _query_info_by_id_range(self, statement, province_code, city_code, town_code, begin_id, end_id):
        await self._get_database_client()

        content = await self._database_client.query_all(statement=statement,
                                                        parameter=dict(
                                                                    province_code=province_code,
                                                                    city_code=city_code,
                                                                    town_code=town_code,
                                                                    begin_id=begin_id,
                                                                    end_id=end_id))

        return content
