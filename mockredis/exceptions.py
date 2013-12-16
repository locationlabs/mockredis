"""
Emulates exceptions raised by the Redis client.
"""


class RedisError(Exception):
    pass


class ResponseError(Exception):
    pass


class WatchError(Exception):
    pass
