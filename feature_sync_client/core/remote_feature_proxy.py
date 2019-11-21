#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Dict, List, Any

from lib.async_http_response_proxy import AsyncHttpClientProxy, AsyncHttpResponseProxy

from core.conf_parameter import g_conf_parameter


class RemoteFeatureProxy(object):
    def __init__(self):
        self.feature_server_url = g_conf_parameter.remote_feature_url

    async def get_sync_status(self, province_code, city_code, town_code) -> Dict[Any, Any]:
        """
        获取远程的状态
        :return:
        """
        client = AsyncHttpClientProxy()
        response = await client.get(self.feature_server_url, command="get_sync_status", province_code=province_code,
                                    city_code=city_code, town_code=town_code)
        response_data = response.json
        return response_data["data"]

    async def query_user_id_range(self, province_code, city_code, town_code, begin_id, end_id):
        return await self._query_info_by_id_range("query_user_id_range", province_code, city_code, town_code, begin_id, end_id)

    async def query_user_range(self, province_code, city_code, town_code, begin_id, end_id):
        return await self._query_info_by_id_range("query_user_range", province_code, city_code, town_code, begin_id, end_id)

    async def add_user(self, values):
        client = AsyncHttpClientProxy()
        request_data = dict(
            command="add_user",
            values=values
        )
        await client.post(self.feature_server_url, json=request_data)

    async def del_user_by_id(self, user_id):
        client = AsyncHttpClientProxy()
        request_data = dict(
            command="del_user_by_id",
            id=user_id
        )
        await client.post(self.feature_server_url, json=request_data)

    async def update_user(self, values):
        self._print("update_user", values)

        client = AsyncHttpClientProxy()
        request_data = dict(
            command="update_user",
            values=values
        )
        await client.post(self.feature_server_url, json=request_data)

    async def query_feature_model_0330_id_range(self, province_code, city_code, town_code, begin_id, end_id):
        return await self._query_info_by_id_range("query_feature_model_0330_id_range", province_code, city_code, town_code, begin_id, end_id)

    async def query_feature_model_0330_time_range(self, province_code, city_code, town_code, begin_id, end_id):
        return await self._query_info_by_id_range("query_feature_model_0330_time_range", province_code, city_code, town_code, begin_id, end_id)

    async def query_feature_model_0330_range(self, province_code, city_code, town_code, begin_id, end_id):
        return await self._query_info_by_id_range("query_feature_model_0330_range", province_code, city_code, town_code, begin_id, end_id)
         
    async def add_feature_model_0330(self, values):
        client = AsyncHttpClientProxy()
        request_data = dict(
            command="add_feature_model_0330",
            values=values
        )
        await client.post(self.feature_server_url, json=request_data)

    async def del_feature_model_0330_by_id(self, feature_id):
        self._print("del_feature_model_0330_by_id", feature_id)

        client = AsyncHttpClientProxy()
        request_data = dict(
            command="del_feature_model_0330_by_id",
            id=feature_id
        )
        await client.post(self.feature_server_url, json=request_data)

    async def update_feature_model_0330(self, values):
        self._print("update_feature_model_0330", values)

        client = AsyncHttpClientProxy()
        request_data = dict(
            command="update_feature_model_0330",
            values=values
        )
        await client.post(self.feature_server_url, json=request_data)

    async def _query_info_by_id_range(self, command, province_code, city_code, town_code, begin_id, end_id):
        client = AsyncHttpClientProxy()
        response = await client.get(self.feature_server_url, command=command,
                        province_code=province_code, city_code=city_code, town_code=town_code, begin_id=begin_id, end_id=end_id)
        response_data = response.json
        return response_data["data"]

    def _print(self, func, data):
        print("============ in {} ==========".format(func))
        print(data)
        print("============ out {} ==========".format(func))