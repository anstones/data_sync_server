#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import traceback
import asyncio
from typing import Dict, List, Any
import os

from lib.logger import logger

from core.conf_parameter import g_conf_parameter
from core.cloud_feature_proxy import CloudFeatureProxy
from core.remote_feature_proxy import RemoteFeatureProxy


class SyncStatus(object):
    def __init__(self, user_id, feature_model_0330_id):
        self.user_id = user_id
        self.feature_model_0330_id = feature_model_0330_id

    def __str__(self):
        return "<SyncStatus id:{}, user_id:{}, feature_model_0330_id: {}>".format(
                id(self), self.user_id, self.feature_model_0330_id)


class FeatureProcessor(object):
    def __init__(self):
        self.province_code = g_conf_parameter.sync_province_code
        self.city_code = g_conf_parameter.sync_city_code
        self.town_code = g_conf_parameter.sync_town_code

        self._cloud_feature_proxy = CloudFeatureProxy()
        self._remote_feature_proxy = RemoteFeatureProxy()

        self.stop = False
        self.running = False

    async def start(self):
        if self.running:
            logger.info("is running, continue")
            return
        now = datetime.datetime.now()
        logger.info("[start]->begin, {}".format(now))

        try:
            self.running = True

            cloud_status = await self._cloud_feature_proxy.get_sync_status(self.province_code, self.city_code, self.town_code)
            cloud_status = SyncStatus(**cloud_status)
            logger.info("cloud_status: {}".format(cloud_status))

            remote_status = await self._remote_feature_proxy.get_sync_status(self.province_code, self.city_code, self.town_code)
            remote_status = SyncStatus(**remote_status)
            logger.info("remote_status: {}".format(remote_status))
            
            await self.sync_user(cloud_status, remote_status)
            await self.sync_feature_model_0330(cloud_status, remote_status)
        finally:
            self.running = False
            end = datetime.datetime.now()
            logger.info("[start]->end, {} ,cost {}s".format(end, (end-now).seconds))

    async def sync_user(self, cloud_status: SyncStatus, remote_status: SyncStatus):
        now = datetime.datetime.now()
        logger.info("[sync_user]->start, begin:{}".format(now))

        await self._user_check_for_insert(cloud_status, remote_status)
        await self._user_check_for_insert_miss(cloud_status, remote_status)
        await self._user_check_for_del(cloud_status, remote_status)
        await self._user_check_for_update(cloud_status, remote_status)

        end = datetime.datetime.now()
        logger.info("[sync_user]->end, end:{}, cost:{}s".format(end, round((end-now).total_seconds(),3)))

    async def sync_feature_model_0330(self, cloud_status: SyncStatus, remote_status: SyncStatus):
        now = datetime.datetime.now()
        logger.info("start sync feature model 0330, begin:{}".format(now))

        await self._feature_model_0330_check_for_insert(cloud_status, remote_status)
        await self._feature_model_0330_check_for_insert_miss(cloud_status, remote_status)
        await self._feature_model_0330_check_for_delete(cloud_status, remote_status)
        await self._feature_model_0330_check_for_update(cloud_status, remote_status)

        end = datetime.datetime.now()
        logger.info(
            "end sync feature model 0330, end:{}, cost:{}s".format(end, round((end-now).total_seconds(),3)))

    async def _user_check_for_insert(self, cloud_status: SyncStatus, remote_status: SyncStatus):
        end_id = cloud_user_id = cloud_status.user_id
        old_begin_id = begin_id = remote_user_id = remote_status.user_id

        logger.info("[_user_check_for_insert]->begin, cloud: {}, remote: {}".format(end_id, begin_id))

        if begin_id < end_id:
            while begin_id < end_id:
                records = await self._cloud_feature_proxy.query_user_range(
                    self.province_code, self.city_code, self.town_code, begin_id, end_id)
                logger.info("user_check_for_insert: {} {} {} {} result_len: {}".format(
                    self.province_code, self.city_code, self.town_code, begin_id, end_id, len(records)))

                if not records:
                    logger.info("no records, for insert, break")
                    break
                else:
                    for record in records:  # type: Dict[Any, Any]
                        begin_id = record["id"]
                        logger.debug("[insert_feature_model_0330]->begin {}".format(begin_id))
                        logger.debug("prepare insert data {}".format(record))
                        await self._remote_feature_proxy.add_user(record)
                        logger.debug("[insert_feature_model_0330]->end {}".format(begin_id))
        else:
            logger.info("begin_id >= end_id, begin_id:{}, end_id:{}".format(begin_id, end_id))
        logger.info("[_user_check_for_insert]->end, ({}, {})".format(old_begin_id, end_id))

    async def _user_check_for_insert_miss(self, cloud_status: SyncStatus, remote_status: SyncStatus):

        def get_ids(user_records: List[Dict[Any, Any]]) -> List[int]:
            return [record["id"] for record in user_records]

        begin_id = -1
        end_id = cloud_status.user_id
        logger.info("[_user_check_for_insert_miss]->begin: (-1, {})".format(end_id))
        if begin_id >= end_id:
            logger.info("begin_id >= end_id, begin_id:{}, end_id:{}".format(begin_id, end_id))
        else:
            while begin_id < end_id:
                cloud_records = await self._cloud_feature_proxy.query_user_range(
                                        self.province_code, self.city_code, self.town_code, begin_id, end_id)
                cloud_ids = set(get_ids(cloud_records))
                if not cloud_ids:
                    break
                cloud_map = {line['id']: line for line in cloud_records}

                remote_records = await self._remote_feature_proxy.query_user_id_range(
                                        self.province_code, self.city_code, self.town_code, begin_id, end_id)
                remote_ids = set(get_ids(remote_records))

                miss_ids = cloud_ids - remote_ids
                if not miss_ids:
                    logger.info("no miss_ids between: range({}, {})".format(begin_id, end_id))
                else:
                    for _id in miss_ids:
                        logger.info("insert user:{}".format(_id))
                        await self._remote_feature_proxy.add_user(cloud_map[_id])
                begin_id = max(cloud_ids)
        logger.info("[_user_check_for_insert_miss]->end: (-1,{})".format(end_id))

    async def _user_check_for_del(self, cloud_status: SyncStatus, remote_status: SyncStatus):

        def get_ids(user_records: List[Dict[Any, Any]]) -> List[int]:
            return [record["id"] for record in user_records]

        begin_id = -1
        end_id = cloud_status.user_id
        logger.info("[_user_check_for_del]->begin: (-1, {})".format(end_id))
        if begin_id >= end_id:
            logger.info("begin_id >= end_id, begin_id:{}, end_id:{}".format(begin_id, end_id))
        else:
            while begin_id < end_id:
                remote_records = await self._remote_feature_proxy.query_user_id_range(
                                        self.province_code, self.city_code, self.town_code, begin_id, end_id)
                remote_ids = set(get_ids(remote_records))
                if not remote_ids:
                    logger.info("not found ids for delete")
                    break
                    
                cloud_records = await self._cloud_feature_proxy.query_user_id_range(
                                        self.province_code, self.city_code, self.town_code, begin_id, end_id)
                cloud_ids = set(get_ids(cloud_records))

                diff_ids = remote_ids - cloud_ids
                if not diff_ids:
                    logger.info("no diff between:range({}, {})".format(begin_id, end_id))
                else:
                    for _id in diff_ids:
                        logger.info("delete user:{}".format(_id))
                        await self._remote_feature_proxy.del_user_by_id(_id)
                begin_id = max(remote_ids)
        logger.info("[_user_check_for_del]->end: (-1,{})".format(end_id))

    async def _user_check_for_update(self, cloud_status: SyncStatus, remote_status: SyncStatus):
        """
        检测更新操作
        """

        def get_ids(user_records: List[Dict[Any, Any]]) -> List[int]:
            return [record['id'] for record in user_records]

        begin_id = -1
        end_id = cloud_status.user_id
        logger.info("[_user_check_for_update]->begin: (-1, {})".format(end_id))

        if begin_id >= end_id:
            logger.info("begin_id >= end_id, begin_id:{}, end_id:{}".format(begin_id, end_id))
        else:
            while begin_id < end_id:
                remote_records = await self._remote_feature_proxy.query_user_range(
                                        self.province_code, self.city_code, self.town_code, begin_id, end_id)
                remote_map = {line['id']: line for line in remote_records}
                remote_ids = set(get_ids(remote_records))
                if not remote_ids:
                    logger.info("not found ids for update")
                    break

                cloud_records = await self._cloud_feature_proxy.query_user_range(
                                        self.province_code, self.city_code, self.town_code, begin_id, end_id)
                cloud_map = {line['id']: line for line in cloud_records}
                cloud_ids = set(get_ids(cloud_records))

                intersection_ids = remote_ids.intersection(cloud_ids)
                if not intersection_ids:
                    logger.info(
                        "no intersection ids between:range({}, {})".format(begin_id, end_id))
                else:
                    for _id in intersection_ids:
                        cloud_instance = cloud_map[_id]
                        remote_instance = remote_map[_id]

                        for field, cloud_field_value in cloud_instance.items():
                            if field == "id":
                                continue
                            remote_field_value = remote_instance[field]
                            if remote_field_value != cloud_field_value:
                                logger.info("update user: {}, key: {}, cloud_field_value: {}, remote_field_value: {}".format(
                                        _id, field, cloud_field_value, remote_field_value))
                                await self._remote_feature_proxy.update_user(cloud_instance)
                                break

                begin_id = max(remote_ids)
        logger.info("[_user_check_for_update]->end: (-1,{})".format(end_id))

    async def _feature_model_0330_check_for_insert(self, cloud_status: SyncStatus, remote_status: SyncStatus):
        begin_id = remote_status.feature_model_0330_id
        end_id = cloud_status.feature_model_0330_id
        logger.info("[_feature_model_0330_check_for_insert]->begin: ({}, {})".format(begin_id, end_id))

        if begin_id >= end_id:
            logger.info("feature_check_for_insert, begin_id >= end_id, begin_id:{}, end_id:{}".format(begin_id, end_id))
        else:
            while begin_id < end_id:
                rows = await self._cloud_feature_proxy.query_feature_model_0330_range(
                                self.province_code, self.city_code, self.town_code, begin_id, end_id)
                logger.debug("query_data:{} {} {} {}, count:{}".format(
                        self.province_code, self.city_code, self.town_code, begin_id, end_id, len(rows)))
                if not rows:
                    break

                for row in rows:
                    begin_id = row["id"]
                    logger.debug("[insert_feature_model_0330]->begin {}".format(begin_id))
                    logger.debug("prepare insert data:{}".format(row))
                    await self._remote_feature_proxy.add_feature_model_0330(row)
                    logger.debug("[insert_feature_model_0330]->end {}".format(begin_id))

        logger.info("[_feature_model_0330_check_for_insert]->end: ({}, {})".format(begin_id, end_id))

    async def _feature_model_0330_check_for_insert_miss(self, cloud_status: SyncStatus, remote_status: SyncStatus):

        def get_ids(user_records: List[Dict[Any, Any]]) -> List[int]:
            return [record["id"] for record in user_records]

        begin_id = -1
        end_id = cloud_status.user_id
        logger.info("[_feature_model_0330_check_for_insert_miss]->begin: (-1, {})".format(end_id))
        if begin_id >= end_id:
            logger.info("begin_id >= end_id, begin_id:{}, end_id:{}".format(begin_id, end_id))
        else:
            while begin_id < end_id:
                cloud_records = await self._cloud_feature_proxy.query_feature_model_0330_range(
                                        self.province_code, self.city_code, self.town_code, begin_id, end_id)
                cloud_ids = set(get_ids(cloud_records))
                if not cloud_ids:
                    break
                cloud_map = {line['id']: line for line in cloud_records}

                remote_records = await self._remote_feature_proxy.query_feature_model_0330_id_range(
                                        self.province_code, self.city_code, self.town_code, begin_id, end_id)
                remote_ids = set(get_ids(remote_records))

                miss_ids = cloud_ids - remote_ids
                if not miss_ids:
                    logger.info("no miss_ids between: range({}, {})".format(begin_id, end_id))
                else:
                    for _id in miss_ids:
                        logger.info("insert feature_model_0330:{}".format(_id))
                        await self._remote_feature_proxy.add_feature_model_0330(cloud_map[_id])
                begin_id = max(cloud_ids)
        logger.info("[_feature_model_0330_check_for_insert_miss]->end: (-1,{})".format(end_id))

    async def _feature_model_0330_check_for_delete(self, cloud_status: SyncStatus, remote_status: SyncStatus):
        def get_ids(feature_records: List[Dict[Any, Any]]) -> List[int]:
            return [record['id'] for record in feature_records]

        begin_id = -1
        end_id = remote_status.feature_model_0330_id
        logger.info("[_feature_model_0330_check_for_delete]->begin, (-1, {})".format(end_id))

        while begin_id < end_id:
            remote_records = await self._remote_feature_proxy.query_feature_model_0330_id_range(
                                    self.province_code, self.city_code, self.town_code, begin_id, end_id)
            remote_ids = set(get_ids(remote_records))
            if not remote_ids:
                logger.info("not found ids for ({}, {})".format(begin_id, end_id))
                break

            cloud_records = await self._cloud_feature_proxy.query_feature_model_0330_id_range(
                                    self.province_code, self.city_code, self.town_code, begin_id, end_id)
            cloud_ids = set(get_ids(cloud_records))

            diff_ids = remote_ids - cloud_ids
            if not diff_ids:
                logger.info("no diff between:range({}, {})".format(begin_id, end_id))
            else:
                for _id in diff_ids:
                    logger.info("delete feature_model_0330:{id}".format(_id))
                    await self._remote_feature_proxy.del_feature_model_0330_by_id(_id)
            begin_id = max(remote_ids)

        logger.info("[_feature_model_0330_check_for_delete]->end, (-1, {})".format(end_id))
    
    async def _feature_model_0330_check_for_update(self, cloud_status: SyncStatus, remote_status: SyncStatus):
        """
        检测更新操作
        """

        def get_ids(user_records: List[Dict[Any, Any]]) -> List[int]:
            return [record['id'] for record in user_records]

        begin_id = -1
        end_id = cloud_status.user_id
        logger.info("[_user_check_for_update]->begin: (-1, {})".format(end_id))

        if begin_id >= end_id:
            logger.info("begin_id >= end_id, begin_id:{}, end_id:{}".format(begin_id, end_id))
        else:
            while begin_id < end_id:
                remote_records = await self._remote_feature_proxy.query_feature_model_0330_time_range(
                                        self.province_code, self.city_code, self.town_code, begin_id, end_id)
                remote_map = {line['id']: line for line in remote_records}
                remote_ids = set(get_ids(remote_records))
                if not remote_ids:
                    logger.info("not found ids for update")
                    break

                cloud_records = await self._cloud_feature_proxy.query_feature_model_0330_range(
                                        self.province_code, self.city_code, self.town_code, begin_id, end_id)
                cloud_map = {line['id']: line for line in cloud_records}
                cloud_ids = set(get_ids(cloud_records))

                intersection_ids = remote_ids.intersection(cloud_ids)
                if not intersection_ids:
                    logger.info(
                        "no intersection ids between:range({}, {})".format(begin_id, end_id))
                else:
                    for _id in intersection_ids:
                        cloud_instance = cloud_map[_id]
                        remote_instance = remote_map[_id]

                        cloud_timestamp = cloud_instance.get("timestamp", -1)
                        remote_timestamp = remote_instance.get("timestamp", -2)

                        if remote_timestamp != cloud_timestamp:
                            logger.info("update feature_model_0330: {}, cloud_timestamp: {}, remote_timestamp: {}".format(
                                        _id, cloud_timestamp, remote_timestamp))
                            await self._remote_feature_proxy.update_feature_model_0330(cloud_instance)

                begin_id = max(remote_ids)
        logger.info("[_user_check_for_update]->end: (-1,{})".format(end_id))