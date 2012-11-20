from unittest import TestCase, skipUnless, skip
from mockredis import MockRedis

try:
    from redis import Redis, RedisError
    Redis(db=15).echo("test")
except (ImportError, RedisError):
    Redis = False  # NOQA


key = 'MOCKREDIS-key-{test}'
attr = 'attr-{test}'

values = [
    1,
    '1',
    '12.123123',
    -123,
    -122,
    "-1234.123",
    "some text",
    [1, 3, 5, '12312', 'help'],
    {'key':'value', 'key2':12}
]


redis_writes = dict(

    set=dict(
        params=[
            ["{key}", "{value}"],
        ],
        keys=["simple"],
        values=values
    ),
    hset=dict(
        params=[
            ["{key}", "{attr}", "{value}"],
        ],
        keys=["hashset"],
        values=values,
        attr=[str(r) for r in range(10)]
    ),
    sadd=dict(
        params=[
            ["{key}", "{value}"],
        ],
        keys=["set"],
        values=values
    )
)

redis_reads = dict(

    get=dict(
        params=[
            ["{key}"]
        ],
        keys=["simple"]  # keys to read from
    ),
    hget=dict(
        params=[
            ["{key}", "{attr}"]
        ],
        keys=["hashset"],
        attr=[str(r) for r in range(10)]
    ),
    smembers=dict(
        params=[
            ["{key}"]
        ],
        keys=["set", "empty"]
    )
)


@skipUnless(Redis, "redis-py or localbost redis-server not found")
class TestReads(TestCase):

    def setUp(self):
        self.redis = Redis(db=15)
        self.mockredis = MockRedis()

    def tearDown(self):

        test_keys = self.redis.keys("MOCKREDIS*")
        if len(test_keys) > 0:
            self.redis.delete(*test_keys)

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

        self.assertEqual(
            getattr(self.redis, method)(*params),
            getattr(self.mockredis, method)(*params),
            "testing {method}".format(method=method)
        )

    def test_write(self):
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

        self.test_write()

        for method, details in redis_reads.items():
            for m, params in self.gather_params(method, details):
                    self.assert_execute(method, params)

    def gather_params(self, method, details):
        for param in details["params"]:
            for use_key in details["keys"]:

                param = [p.format(key=key.format(test=use_key),
                                  attr=attr.format(test=use_key))
                         for p in param]

                yield method, param

    def gather_read_params(self, method, details):
        for param in details["params"]:
            for use_key in details["keys"]:

                param = [p.format(key=key.format(test=use_key),
                                  attr=attr.format(test=use_key))
                         for p in param]

                yield method, param

    @skip("issue with using yeild in tests")
    def test_reads_clean(self):
        '''
        equality of responses on clean db for redis/mockredis
        '''

        for method, details in redis_reads.items():
            for m, params in self.gather_params(method, details):
                    self.assert_execute(method, params)
