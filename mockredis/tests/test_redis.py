from unittest import TestCase
from mockredis import MockRedis


class TestRedis(TestCase):

    def setUp(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_get(self):
        self.assertEqual(None, self.redis.get('key'))

        self.redis.redis['key'] = 'value'
        self.assertEqual('value', self.redis.get('key'))

    def test_set(self):
        self.assertEqual(None, self.redis.redis.get('key'))

        self.redis.set('key', 'value')
        self.assertEqual('value', self.redis.redis.get('key'))

    def test_get_types(self):
        '''
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
            print self.redis.hget('hkey', 'attr')
            self.assertEqual(value[1],
                             self.redis.hget('hkey', 'attr'),
                             "redis.hincrby")

            self.redis.flushdb()

    def test_ttl(self):
        self.redis.set('key', 'key')
        self.redis.expire('key', 30)

        assert self.redis.ttl('key') <= 30
        self.assertEqual(self.redis.ttl('invalid_key'), -1)

    def test_push_pop_returns_str(self):
        key = 'l'
        values = ['5', 5, [], {}]
        for v in values:
            self.redis.rpush(key, v)
            self.assertEquals(self.redis.lpop(key),
                              str(v))
