#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Dict, Any
import base64
import json

from lib.utils import single_ton, common_result
from lib.mysql_client import MysqlClient
from lib.logger import logger

from core.conf_parameter import g_conf_parameter


@single_ton
class FeatureDatabase(object):
    stmt_get_max_user_id = "select MAX(`id`) as id from user where province_code=%(province_code)s and city_code=%(city_code)s \
                            and (%(town_code)s='-1' or town_code=%(town_code)s)"
    stmt_add_user = "insert user (id,uid,pic_md5,province_code,town_code,city_code) \
                        values (%(id)s,%(uid)s,%(pic_md5)s,%(province_code)s,%(town_code)s,%(city_code)s)"
    stmt_query_user_id_range = "select id from user \
                            where province_code=%(province_code)s and city_code=%(city_code)s \
                            and (%(town_code)s='-1' or town_code=%(town_code)s) \
                            and id>%(begin_id)s and id<=%(end_id)s LIMIT 1000"
    stmt_query_user_range = "select * from user \
                            where province_code=%(province_code)s and city_code=%(city_code)s \
                            and (%(town_code)s='-1' or town_code=%(town_code)s) \
                            and id>%(begin_id)s and id<=%(end_id)s LIMIT 1000"
    stmt_del_user_by_id = "delete from user where id=%(id)s"
    stmt_update_user = "update user set \
                        province_code=%(province_code)s, city_code=%(city_code)s, uid=%(uid)s, \
                        town_code=%(town_code)s, pic_md5=%(pic_md5)s where id=%(id)s"

    stmt_get_max_feature_model_0330_id = "select MAX(`id`) as id from feature_model_0330 \
                                            where province_code=%(province_code)s and city_code=%(city_code)s \
                                            and (%(town_code)s='-1' or town_code=%(town_code)s)"
    stmt_query_feature_model_0330_id_range = "select id from feature_model_0330 \
                                            where province_code=%(province_code)s and city_code=%(city_code)s \
                                            and (%(town_code)s='-1' or town_code=%(town_code)s) \
                                            and id>%(begin_id)s and id<=%(end_id)s LIMIT 1000"
    stmt_query_feature_model_0330_time_range = "select id, timestamp from feature_model_0330 \
                                            where province_code=%(province_code)s and city_code=%(city_code)s \
                                            and (%(town_code)s='-1' or town_code=%(town_code)s) \
                                            and id>%(begin_id)s and id<=%(end_id)s LIMIT 1000"
    stmt_del_feature_model_0330_by_id = "delete from feature_model_0330 where id=%(id)s"
    stmt_get_feature_model_0330_by_user_id = "select * from feature_model_0330 where user_id =%(user_id)s limit 1"
    stmt_add_feature_model_0330 = "insert feature_model_0330 (id,user_id,timestamp,feature_id,feature,province_code,city_code,town_code) \
                                    values (%(id)s,%(user_id)s,%(timestamp)s,%(feature_id)s,%(feature)s,%(province_code)s,%(city_code)s,%(town_code)s)"
    stmt_update_feature_model_0330 = "update feature_model_0330 set \
                                        user_id=%(user_id)s, timestamp=%(timestamp)s, feature_id=%(feature_id)s, feature=%(feature)s,\
                                        province_code=%(province_code)s, city_code=%(city_code)s, town_code=%(town_code)s \
                                        where id=%(id)s"

    def __init__(self):
        self._database_client = None
        self._request_handlers = {
            "get_sync_status": self.get_sync_status,

            "query_user_id_range": self.query_user_id_range,
            "query_user_range": self.query_user_range,
            "add_user": self.add_user,
            "del_user_by_id": self.del_user_by_id,
            "update_user": self.update_user,

            "query_feature_model_0330_id_range": self.query_feature_model_0330_id_range,
            "query_feature_model_0330_time_range": self.query_feature_model_0330_time_range,
            "add_feature_model_0330": self.add_feature_model_0330,
            "del_feature_model_0330_by_id": self.del_feature_model_0330_by_id,
            "update_feature_model_0330": self.update_feature_model_0330,
        }
        self._ssl_context = None

    async def _initial_database_client(self):
        """
        初始化数据库连接
        """
        self._database_client = MysqlClient()
        if g_conf_parameter.mysql_use_ssl == 1 and self._ssl_context == None:
            import ssl
            self._ssl_context = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
        self._database_client.initialize(
                                    host=g_conf_parameter.mysql_host,
                                    port=g_conf_parameter.mysql_port,
                                    user=g_conf_parameter.mysql_user,
                                    password=g_conf_parameter.mysql_password,
                                    db=g_conf_parameter.mysql_db,
                                    ssl=self._ssl_context)
        await self._database_client.create_pool()

    async def dispatch_request(self, data):
        command = data.get("command", "")

        if self._database_client is None:
            await self._initial_database_client()
            
        return await self._request_handlers.get(command, self.handler_default)(data)

    async def handler_default(self, data):
        return common_result(code=101, desc="no support command <{}>".format(data.get("command", "")), data=data)

    async def get_sync_status(self, data):
        """
        获取同步状态
        :return:
        """
        max_feature_model_0330_id = await self._database_client.query_one(self.stmt_get_max_feature_model_0330_id,
                                                                            parameter=data)
        max_feature_model_0330_id = max_feature_model_0330_id["id"]
        if max_feature_model_0330_id is None:
            max_feature_model_0330_id = -1

        max_user_id = await self._database_client.query_one(self.stmt_get_max_user_id,
                                                            parameter=data)
        max_user_id = max_user_id["id"]
        if max_user_id is None:
            max_user_id = -1

        ret = {
            'user_id': max_user_id,
            'feature_model_0330_id': max_feature_model_0330_id
        }
        return common_result(data=ret)

    async def query_user_id_range(self, data):
        content = await self._database_client.query_all(self.stmt_query_user_id_range,
                                                        parameter=data)
            
        return common_result(data=content)

    async def query_user_range(self, data):
        content = await self._database_client.query_all(self.stmt_query_user_range,
                                                        parameter=data)

        # content 值 ((7026, '42489ff329f4456a8f241df79de797f1', b'python2的pickle编码的二进制', '510000', '511102', '511100'),)
        for c in content:
            if isinstance(c["pic_md5"], bytes):
                c["pic_md5"] = base64.b64encode(c["pic_md5"]).decode()
            
        return common_result(data=content)

    async def add_user(self, data):
        values = data.get("values")

        # 为了方便传输，values["pic_md5"] 是经过编码的 base64.b64encode(c["pic_md5"]).decode()，这里解码
        if values["pic_md5"] is not None:
            values["pic_md5"] = base64.b64decode(values["pic_md5"].encode())

        row_count, _ = await self._database_client.execute(self.stmt_add_user, parameter=values)
        assert row_count > 0, "fail to insert user:{}".format(values)

        logger.debug("insert data {} success".format(values))
        return common_result(data=row_count)

    async def del_user_by_id(self, data):
        user_id = data.get("id")
        feature_obj = await self._database_client.query_one(self.stmt_get_feature_model_0330_by_user_id,
                                                            parameter=dict(user_id=user_id))
        if feature_obj:
            logger.warn("exists feature model 0330 record for user: {}, so skip del user: {}".format(user_id, data))
            return

        row_count, _ = await self._database_client.execute(self.stmt_del_user_by_id, parameter=data)
        assert row_count > 0, "not found user row with id: {}".format(data)

        logger.debug("delete data {} success".format(data))
        return common_result(data=row_count)

    async def update_user(self, data):
        values = data.get("values")

        # 为了方便传输，values["pic_md5"] 是经过编码的 base64.b64encode(c["pic_md5"]).decode()，这里解码
        if values["pic_md5"] is not None:
            values["pic_md5"] = base64.b64decode(values["pic_md5"].encode())

        row_count, _ = await self._database_client.execute(self.stmt_update_user, parameter=values)
        assert row_count > 0, "fail to update user:{}".format(values)

        logger.debug("update data {} success".format(values))
        return common_result(data=row_count)

    async def query_feature_model_0330_id_range(self, data):
        content = await self._database_client.query_all(self.stmt_query_feature_model_0330_id_range,
                                                        parameter=data)
            
        return common_result(data=content)

    async def query_feature_model_0330_time_range(self, data):
        content = await self._database_client.query_all(self.stmt_query_feature_model_0330_time_range,
                                                        parameter=data)
            
        return common_result(data=content)

    async def add_feature_model_0330(self, data):
        values = data.get("values")
        if values["feature"] is not None:
            values["feature"] = values["feature"].encode()

        row_count, _ = await self._database_client.execute(self.stmt_add_feature_model_0330, parameter=values)
        assert row_count > 0, "fail to insert feature_model_0330:{}".format(values)

        logger.debug("insert data {} success".format(values))
        return common_result(data=row_count)

    async def del_feature_model_0330_by_id(self, data):
        row_count, _ = await self._database_client.execute(self.stmt_del_feature_model_0330_by_id, parameter=data)
        assert row_count > 0, "not found feature_model_0330 row with id: {}".format(data)
        return common_result(data=row_count)

    async def update_feature_model_0330(self, data):
        values = data.get("values")
        if values["feature"] is not None:
            values["feature"] = values["feature"].encode()

        row_count, _ = await self._database_client.execute(self.stmt_update_feature_model_0330, parameter=values)
        assert row_count > 0, "fail to update feature_model_0330:{}".format(values)

        logger.debug("update data {} success".format(values))
        return common_result(data=row_count)
