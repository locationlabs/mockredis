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
        testing type coversions for set/get, hset/hget, sadd/smembers

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

        assert self.redis.ttl('key') <= 30
        self.assertEqual(self.redis.ttl('invalid_key'), -1)

    def test_push_pop_returns_str(self):
        key = 'l'
        values = ['5', 5, [], {}]
        for v in values:
            self.redis.rpush(key, v)
            self.assertEquals(self.redis.lpop(key),
                              str(v))

    def test_zadd(self):
        key = "zset"
        values = [("one", 1), ("uno", 1), ("two", 2), ("three", 3)]
        for member, score in values:
            self.assertEquals(1, self.redis.zadd(key, member, score))

    def test_zadd_strict(self):
        """Argument order for zadd depends on strictness"""
        self.redis.strict = True
        key = "zset"
        values = [("one", 1), ("uno", 1), ("two", 2), ("three", 3)]
        for member, score in values:
            self.assertEquals(1, self.redis.zadd(key, score, member))

    def test_zadd_duplicate_key(self):
        key = "zset"
        self.assertEquals(1, self.redis.zadd(key, "one", 1.0))
        self.assertEquals(0, self.redis.zadd(key, "one", 2.0))

    def test_zadd_wrong_type(self):
        key = "zset"
        self.redis.set(key, "value")
        with self.assertRaises(Exception):
            self.redis.zadd(key, "one", 2.0)

    def test_zadd_multiple_bad_args(self):
        key = "zset"
        args = ["one", 1, "two"]
        with self.assertRaises(Exception):
            self.redis.zadd(key, *args)

    def test_zadd_multiple_bad_score(self):
        key = "zset"
        with self.assertRaises(Exception):
            self.redis.zadd(key, "one", "two")

    def test_zadd_multiple_args(self):
        key = "zset"
        args = ["one", 1, "uno", 1, "two", 2, "three", 3]
        self.assertEquals(4, self.redis.zadd(key, *args))

    def test_zadd_multiple_kwargs(self):
        key = "zset"
        kwargs = {"one": 1, "uno": 1, "two": 2, "three": 3}
        self.assertEquals(4, self.redis.zadd(key, **kwargs))

    def test_zcard(self):
        key = "zset"
        self.assertEquals(0, self.redis.zcard(key))
        self.redis.zadd(key, "one", 1)
        self.assertEquals(1, self.redis.zcard(key))
        self.redis.zadd(key, "one", 2)
        self.assertEquals(1, self.redis.zcard(key))
        self.redis.zadd(key, "two", 2)
        self.assertEquals(2, self.redis.zcard(key))

    def test_zincrby(self):
        key = "zset"
        self.assertEquals(1.0, self.redis.zincrby(key, "member1"))
        self.assertEquals(2.0, self.redis.zincrby(key, "member2", 2))
        self.assertEquals(-1.0, self.redis.zincrby(key, "member1", -2))

    def test_zrange(self):
        key = "zset"
        self.assertEquals([], self.redis.zrange(key, 0, -1))
        self.redis.zadd(key, "one", 1.5)
        self.redis.zadd(key, "two", 2.5)
        self.redis.zadd(key, "three", 3.5)

        # full range
        self.assertEquals(["one", "two", "three"],
                          self.redis.zrange(key, 0, -1))
        # withscores
        self.assertEquals([("one", 1.5), ("two", 2.5), ("three", 3.5)],
                          self.redis.zrange(key, 0, -1, withscores=True))
        # score_cast_func
        self.assertEquals([("one", 1), ("two", 2), ("three", 3)],
                          self.redis.zrange(key, 0, -1, withscores=True, score_cast_func=int))

        # positive ranges
        self.assertEquals(["one"], self.redis.zrange(key, 0, 0))
        self.assertEquals(["one", "two"], self.redis.zrange(key, 0, 1))
        self.assertEquals(["one", "two", "three"], self.redis.zrange(key, 0, 2))
        self.assertEquals(["one", "two", "three"], self.redis.zrange(key, 0, 3))
        self.assertEquals(["two", "three"], self.redis.zrange(key, 1, 2))
        self.assertEquals(["three"], self.redis.zrange(key, 2, 3))

        # negative ends
        self.assertEquals(["one", "two", "three"], self.redis.zrange(key, 0, -1))
        self.assertEquals(["one", "two"], self.redis.zrange(key, 0, -2))
        self.assertEquals(["one"], self.redis.zrange(key, 0, -3))
        self.assertEquals([], self.redis.zrange(key, 0, -4))

        # negative starts
        self.assertEquals([], self.redis.zrange(key, -1, 0))
        self.assertEquals(["three"], self.redis.zrange(key, -1, -1))
        self.assertEquals(["two", "three"], self.redis.zrange(key, -2, -1))
        self.assertEquals(["one", "two", "three"], self.redis.zrange(key, -3, -1))
        self.assertEquals(["one", "two", "three"], self.redis.zrange(key, -4, -1))
