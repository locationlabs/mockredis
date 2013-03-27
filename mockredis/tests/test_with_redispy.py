from test_configuration import * # flake8: noqa
from mockredis import MockRedis

from unittest import TestCase, skipUnless
import logging


def redis_exists():
    try:
        from redis import StrictRedis as Redis, RedisError
        Redis(db=15).echo("test")
        return True
    except (ImportError, RedisError):
        return False


@skipUnless(redis_exists(), "redis-py or localhost redis-server not found")
class TestReads(TestCase):

    def setUp(self):
        from redis import StrictRedis as Redis

        self.redis = Redis(db=15)
        self.mockredis = MockRedis()
        self.logger = logging.getLogger("mockredis.TestReads")

    def tearDown(self):

        test_keys = self.redis.keys("MOCKREDIS*")
        if len(test_keys) > 0:
            self.redis.delete(*test_keys)
        self.mockredis.flushdb()

    def connections(self):
        self.assertEqual("testing_redis",
                         self.redis.echo("testing_redis"))
        self.assertEqual("testing_redis",
                         self.mockredis.echo("testing_redis"))

    def execute(self, method, params):
        '''
        Execute command on redis and mockredis
        '''

        getattr(self.redis, method)(*params)
        getattr(self.mockredis, method)(*params)

    def assert_execute(self, method, params):
        '''
        Assert that method response is the same
        for redis and mockredis
        '''

        self.logger.debug("method={method},params={params}"
                          .format(method=method, params=params))
        self.assertEqual(
            getattr(self.redis, method)(*params),
            getattr(self.mockredis, method)(*params),
            "testing {method}".format(method=method)
        )

    def write(self):
        '''
        successful writes for redis/mockredis (responses NOT compared)
        '''

        for method, details in redis_writes.items():
            for m, params in self.gather_params(method, details):
                self.execute(method, params)

    def test_reads(self):
        '''
        equality of responses for redis/mockredis
        '''

        self.write()

        for method, details in redis_reads.items():
            for m, params in self.gather_params(method, details):
                    self.assert_execute(method, params)

    def gather_params(self, method, details):
        for param in details["params"]:
            for use_key in details["keys"]:
                if "values" in details:
                    values = details["values"]
                else:
                    values = [""]
                for i in range(len(values)):
                    param = [p.format(key=key.format(test=use_key),
                                      attr=attr.format(test=use_key),
                                      value=values[i],
                                      index=i)
                             for p in param]

                    yield method, param

    def test_reads_clean(self):
        '''
        equality of responses on clean db for redis/mockredis
        '''

        for method, details in redis_reads.items():
            for m, params in self.gather_params(method, details):
                    self.assert_execute(method, params)
