"""
Test fixtures for mockredis using the WithRedis plugin.
"""
from contextlib import contextmanager

from nose.tools import assert_raises, raises

from mockredis.noseplugin import WithRedis


def setup(self):
    """
    Test setup fixtures. Creates and flushes redis/strict redis instances.
    """
    self.redis = WithRedis.Redis()
    self.redis_strict = WithRedis.StrictRedis()
    self.redis.flushdb()
    self.redis_strict.flushdb()


def teardown(self):
    """
    Test teardown fixtures.
    """
    if self.redis:
        del self.redis
    if self.redis_strict:
        del self.redis_strict


def raises_response_error(func):
    """
    Test decorator that handles ResponseError or its mock equivalent
    (currently ValueError).

    mockredis does not currently raise redis-py's exceptions because it
    does not current depend on redis-py strictly.
    """
    return raises(WithRedis.ResponseError)(func)


@contextmanager
def assert_raises_redis_error():
    """
    Test context manager that asserts that a RedisError or its mock equivalent
    (currently `redis.exceptions.RedisError`) were raised.

    mockredis does not currently raise redis-py's exceptions because it
    does not current depend on redis-py strictly.
    """
    with assert_raises(WithRedis.RedisError) as capture:
        yield capture


@contextmanager
def assert_raises_watch_error():
    """
    Test context manager that asserts that a WatchError or its mock equivalent
    (currently `watch.exceptions.WatchError`) were raised.

    mockwatch does not currently raise watch-py's exceptions because it
    does not current depend on watch-py strictly.
    """
    with assert_raises(WithRedis.WatchError) as capture:
        yield capture
