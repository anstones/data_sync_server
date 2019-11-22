#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import aiomysql
from datetime import datetime, date
import traceback

from lib.logger import logger


class MysqlClient(object):
    def __init__(self):
        self._pool = None

    def initialize(self, minsize=1, maxsize=10, loop=None,
                        host="localhost", port=3306, user=None, password="", db=None, charset='utf8',
                        autocommit=True, ssl=None):
        self._minsize = minsize
        self._maxsize = maxsize
        self._loop = loop or asyncio.get_event_loop()
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db = db
        self._charset = charset
        self._autocommit = autocommit
        self._ssl_context = ssl

    async def create_pool(self):
        self._pool = await aiomysql.create_pool(minsize=self._minsize,
                                                maxsize=self._maxsize,
                                                loop=self._loop,
                                                host=self._host,
                                                port=self._port,
                                                user=self._user,
                                                password=self._password,
                                                db=self._db,
                                                charset=self._charset,
                                                autocommit=self._autocommit,
                                                ssl=self._ssl_context)

    async def close(self):
        self._pool.close()
        await self._pool.wait_closed()

    async def query_one(self, statement, parameter=None):
        result = None
        try:
            result = await self._query_one(statement, parameter)
        except Exception:
            logger.error(traceback.format_exc())
            await self.close()
            await self.create_pool()
            result = await self._query_one(statement, parameter)

        return result

    async def query_many(self, statement, parameter=None, size=None):
        result = None
        try:
            result = await self._query_many(statement, parameter, size)
        except Exception:
            logger.error(traceback.format_exc())
            await self.close()
            await self.create_pool()
            result = await self._query_many(statement, parameter, size)

        return result

    async def query_all(self, statement, parameter=None):
        result = None
        try:
            result = await self._query_all(statement, parameter)
        except Exception:
            logger.error(traceback.format_exc())
            await self.close()
            await self.create_pool()
            result = await self._query_all(statement, parameter)

        return result

    async def execute(self, statement, parameter=None):
        result = None
        try:
            result = await self._execute(statement, parameter)
        except Exception:
            logger.error(traceback.format_exc())
            await self.close()
            await self.create_pool()
            result = await self._execute(statement, parameter)

        return result

    async def executemany(self, statement, parameter=None):
        result = None
        try:
            result = await self._executemany(statement, parameter)
        except Exception:
            logger.error(traceback.format_exc())
            await self.close()
            await self.create_pool()
            result = await self._executemany(statement, parameter)

        return result

    async def _query_one(self, statement, parameter=None):
        result = None
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(statement, parameter)
                row_names = [d[0] for d in cursor.description]
                row_values = await cursor.fetchone()
                result = self.zip_dict(row_names, row_values)

        return result

    async def _query_many(self, statement, parameter=None, size=None):
        result = None
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(statement, parameter)
                row_names = [d[0] for d in cursor.description]
                row_values = await cursor.fetchmany(size=size)
                result = [self.zip_dict(row_names, value) for value in row_values]

        return result

    async def _query_all(self, statement, parameter=None):
        result = None
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(statement, parameter)
                row_names = [d[0] for d in cursor.description]
                row_values = await cursor.fetchall()
                result = [self.zip_dict(row_names, value) for value in row_values]

        return result

    async def _execute(self, statement, parameter=None):
        row_count, last_row_id = 0, 0
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(statement, parameter)
                row_count, last_row_id = cursor.rowcount, cursor.lastrowid

        return row_count, last_row_id

    async def _executemany(self, statement, parameter=None):
        row_count, last_row_id = 0, 0
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(statement, parameter)
                row_count, last_row_id = cursor.rowcount, cursor.lastrowid

        return row_count, last_row_id

    @staticmethod
    def zip_dict(key, var):
        t = dict()
        if not key or not var:
            return t

        def time_format(x):
            if isinstance(x, datetime):
                return x.strftime("%Y-%m-%d %X")
            elif isinstance(x, date):
                return x.strftime("%Y-%m-%d")
            else:
                return x

        for i in range(len(key)):
            t[key[i]] = time_format(var[i])
        return t


if __name__ == '__main__':
    async def test_example(loop):
        import uuid
        mysql_client = MysqlClient()
        await mysql_client.create_pool(host='192.168.0.249', port=3306, user="root", db='facefeature', loop=loop)

        uid = uuid.uuid4().hex
        r = await mysql_client.execute("insert into user(uid, pic_md5, province_code) value(%(uid)s, %(pic_md5)s, %(province_code)s)",
                                       parameter={"uid": uid, "pic_md5": b"xxxx", "province_code": "20000"})
        print(r)

        c = await mysql_client.query_one('select * from user where uid = %(uid)s', parameter={"uid": uid})
        print(c)

        c = await mysql_client.query_many('select * from user', size=3)
        print(c)

        c = await mysql_client.query_all('select * from user')
        print(c)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_example(loop))
