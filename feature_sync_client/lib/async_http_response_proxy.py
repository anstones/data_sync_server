#!/usr/bin/env python
# -*- coding: utf-8 -*-

import aiohttp
import json as _json

from lib.exceptions import CallServiceException


class AsyncHttpResponseProxy(object):
    def __init__(self, response=None, content=None):
        self._response = response
        self._content = content

    @property
    def method(self):
        return self._response.method

    @property
    def url(self):
        return self._response.url

    @property
    def host(self):
        return self._response.host

    @property
    def status(self):
        return self._response.status

    @property
    def headers(self):
        return self._response.headers

    @property
    def request_info(self):
        return self._response.request_info

    @property
    def response(self):
        return self._response

    async def set_response(self, response):
        self._response = response
        self._content = await response.read()

    @property
    def content(self):
        return self._content

    @property
    def text(self):
        return self._content.decode()

    @property
    def json(self):
        stripped = self._content.strip()
        if not stripped:
            return None

        return _json.loads(stripped.decode())


class AsyncHttpClientProxy(object):
    @staticmethod
    async def get(url, *, params=None, **kwargs):
        if params is not None:
            kwargs.update(params)

        response = AsyncHttpResponseProxy()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=kwargs) as resp:
                    await response.set_response(resp)
        except Exception as e:
            raise CallServiceException(method="GET", url=url, errmsg=e)

        return response

    @staticmethod
    async def post(url, *, params=None, data=None, json=None, headers=None, **kwargs):
        if json is not None:
            if headers is None:
                headers = {"Content-Type": "application/json; charset=UTF-8"}
            data = _json.dumps(json)

        response = AsyncHttpResponseProxy()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data, params=params, headers=headers, **kwargs) as resp:
                    await response.set_response(resp)
        except Exception as e:
            raise CallServiceException(method="GET", url=url, errmsg=e)

        return response


if __name__ == '__main__':
    import asyncio

    async def test_example():
        url = "http://httpbin.org"
        client = AsyncHttpClientProxy()

        async def test_get():
            resp = await client.get("{}/get".format(url), params={'a': 'hello', 'b': 'world'}, c="lily", d="ss")
            print(resp.text)

        async def test_post():
            resp = await client.post("{}/post".format(url), params={'a': 'hello_post', 'b': 'world_post'}, json={1: 222})
            print(resp.url)
            print(resp.json)

        await test_get()
        await test_post()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_example())
