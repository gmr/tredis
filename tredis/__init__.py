"""
TRedis
======
An asynchronous Redis client for Tornado

"""
from tredis.client import Client, RedisClient
from tredis.exceptions import *
from tredis.strings import BITOP_AND, BITOP_OR, BITOP_XOR, BITOP_NOT

__version__ = '0.8.0'
