#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sanic import Sanic
from sanic.response import json as sa_json
import traceback
import asyncio
import argparse

from lib.logger import logger
from lib.utils import get_log_dir

from core.conf_parameter import g_conf_parameter
from core.feature_database import FeatureDatabase

g_feature_database = FeatureDatabase()


def warpper_response(func):
    async def warpper(*args, **kwargs):
        code, desc, ret = 0, "", {}
        try:
            ret = await func(*args, **kwargs)
        except Exception as err:
            logger.error(traceback.format_exc())
            code, desc = 100, "{}".format(err)

        response = {
            "code": code,
            "desc": desc
        }
        if isinstance(ret, dict):
            response.update(ret)
        else:
            response["data"] = ret
        return sa_json(response)
    return warpper


@warpper_response
async def process_feature_request(request):
    data = request.raw_args if request.method == "GET" else request.json
    ret = await g_feature_database.dispatch_request(data)
    return ret


def main():
    # 解析配置文件
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config_path', default='./conf/feature_sync_server.conf', help='set conf file')
    args = parser.parse_args()
    
    g_conf_parameter.load_conf(args.config_path)

    # 启动日志模块
    logger.set_child_name('feature_sync_server')
    logger.set_file_path(get_log_dir())
    logger.set_file_name(g_conf_parameter.log_file_name)
    logger.set_log_level(g_conf_parameter.log_level)
    logger.set_back_count(g_conf_parameter.log_back_count)
    logger.start()

    app = Sanic("feature_sync_server")
    app.add_route(process_feature_request, "/feature", methods=["GET", "POST"])

    logger.info("{0} feature_sync_server start at port {1} wrokers {2} conf {3} {0}".format(
        "*" * 10, g_conf_parameter.server_port, g_conf_parameter.workers_num, args.config_path))
        
    app.run(host=g_conf_parameter.server_host, port=g_conf_parameter.server_port, 
            workers=g_conf_parameter.workers_num, debug=False, access_log=False)
    

if __name__ == "__main__":
    main()
