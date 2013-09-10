import time
from unittest import TestCase

from mockredis import MockRedis, mock_redis_client, mock_strict_redis_client


class TestFactories(TestCase):

    def test_mock_redis_client(self):
        """
        Test that we can pass kwargs to the Redis mock/patch target.
        """
        self.assertFalse(mock_redis_client(host="localhost", port=6379).strict)

    def test_mock_strict_redis_client(self):
        """
        Test that we can pass kwargs to the StrictRedis mock/patch target.
        """
        self.assertTrue(mock_strict_redis_client(host="localhost", port=6379).strict)


class TestRedis(TestCase):

    def setUp(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_get_types(self):
        '''
        testing type conversions for set/get, hset/hget, sadd/smembers

        Python bools, lists, dicts are returned as strings by
        redis-py/redis.
        '''

        values = list([
            True,
            False,
            [1, '2'],
            {
                'a': 1,
                'b': 'c'
            },
        ])

        self.assertEqual(None, self.redis.get('key'))

        for value in values:
            self.redis.set('key', value)
            self.assertEqual(str(value),
                             self.redis.get('key'),
                             "redis.get")

            self.redis.hset('hkey', 'item', value)
            self.assertEqual(str(value),
                             self.redis.hget('hkey', 'item'))

            self.redis.sadd('skey', value)
            self.assertEqual(set([str(value)]),
                             self.redis.smembers('skey'))

            self.redis.flushdb()

    def test_incr(self):
        '''
        incr, hincr when keys exist
        '''

        values = list([
            (1, '2'),
            ('1', '2'),
        ])

        for value in values:
            self.redis.set('key', value[0])
            self.redis.incr('key')
            self.assertEqual(value[1],
                             self.redis.get('key'),
                             "redis.incr")

            self.redis.hset('hkey', 'attr', value[0])
            self.redis.hincrby('hkey', 'attr')
            self.assertEqual(value[1],
                             self.redis.hget('hkey', 'attr'),
                             "redis.hincrby")

            self.redis.flushdb()

    def test_incr_init(self):
        '''
        incr, hincr, decr when keys do NOT exist
        '''

        self.redis.incr('key')
        self.assertEqual('1', self.redis.get('key'))

        self.redis.hincrby('hkey', 'attr')
        self.assertEqual('1', self.redis.hget('hkey', 'attr'))

        self.redis.decr('dkey')
        self.assertEqual('-1', self.redis.get('dkey'))

    def test_ttl(self):
        self.redis.set('key', 'key')
        self.redis.expire('key', 30)

        result = self.redis.ttl('key')
        # should be an int
        self.assertTrue(isinstance(result, int))
        # should be less than the timeout originally set
        self.assertTrue(result <= 30)

    def test_ttl_when_key_absent(self):
        """Test whether, like the redis-py lib, ttl returns None if the key is absent"""

        self.assertEqual(self.redis.ttl('invalid_key'), None)

    def test_ttl_no_timeout(self):
        """
        Test whether, like the redis-py lib, ttl returns None if the key has no timeout set.
        """
        self.redis.set('key', 'key')
        self.assertEqual(self.redis.ttl('key'), None)

    def test_pttl(self):
        expiration_ms = 3000
        self.redis.set('key', 'key')
        self.redis.pexpire('key', expiration_ms)

        result = self.redis.pttl('key')
        # should be an int
        self.assertTrue(isinstance(result, int))
        # should be less than the timeout originally set
        self.assertTrue(result <= expiration_ms)

    def test_pttl_when_key_absent(self):
        """Test whether, like the redis-py lib, pttl returns None if the key is absent"""

        self.assertEqual(self.redis.pttl('invalid_key'), None)

    def test_pttl_no_timeout(self):
        """
        Test whether, like the redis-py lib, pttl returns None if the key has no timeout set.
        """
        self.redis.set('key', 'key')
        self.assertEqual(self.redis.pttl('key'), None)

    def test_expireat_calculates_time(self):
        """
        test whether expireat sets the correct ttl, setting a timestamp 30s in the future
        """
        self.redis.set('key', 'key')
        self.redis.expireat('key', int(time.time()) + 30)

        result = self.redis.ttl('key')
        # should be an int
        self.assertTrue(isinstance(result, int))
        # should be less than the timeout originally set
        self.assertTrue(result <= 30)

    def test_push_pop_returns_str(self):
        key = 'l'
        values = ['5', 5, [], {}]
        for v in values:
            self.redis.rpush(key, v)
            self.assertEquals(self.redis.lpop(key),
                              str(v))
