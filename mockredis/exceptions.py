"""
Emulates exceptions raised by the Redis client, if necessary.
"""

try:
    # Prefer actual exceptions to defining our own, so code that swaps
    # in implementations does not have to swap in different exception
    # classes.
    from redis.exceptions import RedisError, ResponseError, WatchError
except ImportError:
    class RedisError(Exception):
        pass

    class ResponseError(RedisError):
        pass

    class WatchError(RedisError):
        pass
