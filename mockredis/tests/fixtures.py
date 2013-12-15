"""
Test fixtures for mockredis.

This module includes a nose plugin that allows unit tests to be run with a real
redis-server instance running locally (assuming redis-py) is installed. This provides
a simple way to verify that mockredis tests are accurate (at least for a particular
version of redis-server and redis-py).

For this plugin to work, several things need to be true:

 1. Nose and setuptools need to be used to invoke tests (so the plugin will work).

    Note that the setuptools "entry_point" for "nose.plugins.0.10" must be activated.

 2. A version of redis-py must be installed in the virtualenv under test.

 3. A redis-server instance must be running locally.

 4. The redis-server must have a database that can be flushed between tests.

    YOU WILL LOSE DATA OTHERWISE.

    By default, database 15 is used.

 5. Tests must be written without any references to internal mockredis state. Essentially,
    that means testing GET and SET together instead of separately and looking at the contents
    of `self.redis.redis` (because this won't exist for redis-py).
"""
from functools import partial
import os

from nose.plugins import Plugin
from nose.tools import raises

from mockredis import MockRedis


class WithRedis(Plugin):
    """
    Nose plugin to allow selection of redis-server.
    """
    def options(self, parser, env=os.environ):
        parser.add_option("--use-redis",
                          dest="use_redis",
                          action="store_true",
                          default=False,
                          help="Use a local redis instance to validate tests.")
        parser.add_option("--redis-database",
                          dest="redis_database",
                          default=15,
                          help="Run tests against local redis database")

    def configure(self, options, conf):
        if options.use_redis:
            from redis import Redis, ResponseError, StrictRedis

            WithRedis.Redis = partial(Redis, db=options.redis_database)
            WithRedis.StrictRedis = partial(StrictRedis, db=options.redis_database)
            WithRedis.ResponseError = ResponseError
        else:
            WithRedis.Redis = MockRedis
            WithRedis.StrictRedis = partial(MockRedis, strict=True)
            WithRedis.ResponseError = ValueError


def setup(self):
    """
    Test setup fixtures. Creates and flushes redis/strict redis instances.
    """
    self.redis = WithRedis.Redis()
    self.redis_strict = WithRedis.StrictRedis()
    self.redis.flushdb()
    self.redis_strict.flushdb()


def raises_response_error(func):
    """
    Test decorator that handles ResponseError or its mock equivalent
    (currently ValueError).

    mockredis does not currently raise redis-py's exceptions because it
    does not current depend on redis-py strictly.
    """
    return raises(WithRedis.ResponseError)(func)
