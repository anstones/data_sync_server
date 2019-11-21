#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback
import asyncio
import datetime
import argparse

from lib.logger import logger
from lib.utils import get_log_dir

from core.conf_parameter import g_conf_parameter
from core.feature_processor import FeatureProcessor


class FeatureSyncService(object):
    def __init__(self, event_loop):
        self._event_loop = event_loop
        self._feature_processor = FeatureProcessor()

    def start_service(self):
        try:
            asyncio.ensure_future(self.sync_feature())
        except Exception:
            logger.error(traceback.format_exc())

    async def sync_feature(self, continue_task=True):
        now = datetime.datetime.now()
        logger.info("start sync feature at: {}".format(now))

        next_time = now + datetime.timedelta(seconds=g_conf_parameter.sync_time_interval)

        try:
            await self._feature_processor.start()
        except Exception:
            logger.error(traceback.format_exc())

        if g_conf_parameter.sync_run_once == 1:
            logger.info("only run once so complete")
        else:
            tmp = datetime.datetime.now()
            if tmp > next_time:
                _interval = 0
            else:
                _interval = (next_time - tmp).seconds
            logger.info("sleep {} seconds for next schedule".format(_interval))
            self._event_loop.call_later(_interval, self.start_service)
        
        end = datetime.datetime.now()
        logger.info("end sync feature at: {}, cost:{}s".format(end, round((end-now).total_seconds(), 3)))


def main():
    # 解析配置文件
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config_path', default='./conf/feature_sync_client.conf', help='set conf file')
    args = parser.parse_args()
    
    g_conf_parameter.load_conf(args.config_path)

    # 启动日志模块
    logger.set_child_name('feature_sync_server')
    logger.set_file_path(get_log_dir())
    logger.set_file_name(g_conf_parameter.log_file_name)
    logger.set_log_level(g_conf_parameter.log_level)
    logger.set_back_count(g_conf_parameter.log_back_count)
    logger.start()

    event_loop = asyncio.get_event_loop()
    feature_sync_service = FeatureSyncService(event_loop=event_loop)
    try:
        logger.info("{0} feature_sync_client start use conf file: {1} {0}".format(
                    "*" * 10, args.config_path))
        event_loop.run_until_complete(feature_sync_service.sync_feature())
        event_loop.run_forever()
    except KeyboardInterrupt:
        event_loop.stop()
        event_loop.close()

    logger.info("complete sync or for uncaught exception")
    

if __name__ == "__main__":
    main()
