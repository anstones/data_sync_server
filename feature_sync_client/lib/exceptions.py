#!/usr/bin/env python
# -*- coding: utf-8 -*-

from enum import Enum


class Error(Enum):
    INVALID_MESSAGE = 1
    MISSING_PARAM = 2
    INVALID_PARAM = 3
    SERVER_FAILED = 4
    INTERNAL_FAILED = 5
    INVALID_COMMAND = 6
    SERVICE_FORBID = 7


class ApiBaseException(Exception):
    """
    异常基类
    """
    error_dict = {
        Error.INVALID_MESSAGE.value: "Invalid message format",
        Error.MISSING_PARAM.value: "Missing parameter",
        Error.INVALID_PARAM.value: "Invalid parameter",
        Error.SERVER_FAILED.value: "Request failure",
        Error.INTERNAL_FAILED.value: "Internal failure",
        Error.INVALID_COMMAND.value: "Invalid command",
        Error.SERVICE_FORBID.value: "Forbid service"
    }

    def __init__(self, errcode, errmsg=None):
        """
        :param errcode: 错误代码
        :param errmsg: 错误信息
        """
        self._errmsg = errmsg
        if isinstance(errcode, Error):
            self._errcode = errcode.value
            if self._errmsg is None:
                self._errmsg = self.error_dict.get(errcode.value, "Unknown")
        else:
            self._errcode = errcode

    def _error_message(self):
        error_message = self._errmsg
        if isinstance(self._errmsg, Exception):
            error_message = "{}".format(self._errmsg)
        return error_message

    def __str__(self):
        return '{}({}): {}'.format(self.error_dict.get(self._errcode, "Unknown"), self._errcode, self._error_message())

    __repr__ = __str__


class RequestException(ApiBaseException):
    """
    请求类异常
    """

    def __init__(self, errcode, errmsg=None):
        super(RequestException, self).__init__(errcode, errmsg)


class ServerException(ApiBaseException):
    """
    服务类异常
    """

    def __init__(self, errcode, errmsg=None):
        super(ServerException, self).__init__(errcode, errmsg)


class CallServiceException(ServerException):
    """
    调用远端服务异常
    """

    def __init__(self, method, url, errmsg=None):
        super(CallServiceException, self).__init__(Error.SERVER_FAILED, errmsg)
        self._call_method = method
        self._service_url = url

    @property
    def service_url(self):
        return self._service_url

    def _error_message(self):
        return '{} {} failed, desc({})'.format(
            self._call_method, self._service_url, super(CallServiceException, self)._error_message())


class ForeignKeyExists(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return "<ForeignKeyExists {}>".format(self.msg)
