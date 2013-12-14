from time import time

from nose.tools import assert_raises, eq_, ok_

from mockredis import MockRedis, mock_redis_client, mock_strict_redis_client


class TestFactories(object):

    def test_mock_redis_client(self):
        """
        Test that we can pass kwargs to the Redis mock/patch target.
        """
        ok_(not mock_redis_client(host="localhost", port=6379).strict)

    def test_mock_strict_redis_client(self):
        """
        Test that we can pass kwargs to the StrictRedis mock/patch target.
        """
        ok_(mock_strict_redis_client(host="localhost", port=6379).strict)


class TestRedis(object):

    def setup(self):
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

        eq_(None, self.redis.get('key'))

        for value in values:
            self.redis.set('key', value)
            eq_(str(value),
                self.redis.get('key'),
                "redis.get")

            self.redis.hset('hkey', 'item', value)
            eq_(str(value),
                self.redis.hget('hkey', 'item'))

            self.redis.sadd('skey', value)
            eq_(set([str(value)]),
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
            eq_(value[1],
                self.redis.get('key'),
                "redis.incr")

            self.redis.hset('hkey', 'attr', value[0])
            self.redis.hincrby('hkey', 'attr')
            eq_(value[1],
                self.redis.hget('hkey', 'attr'),
                "redis.hincrby")

            self.redis.flushdb()

    def test_incr_init(self):
        '''
        incr, hincr, decr when keys do NOT exist
        '''

        self.redis.incr('key')
        eq_('1', self.redis.get('key'))

        self.redis.hincrby('hkey', 'attr')
        eq_('1', self.redis.hget('hkey', 'attr'))

        self.redis.decr('dkey')
        eq_('-1', self.redis.get('dkey'))

    def test_ttl(self):
        self.redis.set('key', 'key')
        self.redis.expire('key', 30)

        result = self.redis.ttl('key')
        # should be an int
        ok_(isinstance(result, int))
        # should be less than the timeout originally set
        ok_(result <= 30)

    def test_ttl_when_key_absent(self):
        """Test whether, like the redis-py lib, ttl returns None if the key is absent"""

        eq_(self.redis.ttl('invalid_key'), None)

    def test_ttl_no_timeout(self):
        """
        Test whether, like the redis-py lib, ttl returns None if the key has no timeout set.
        """
        self.redis.set('key', 'key')
        eq_(self.redis.ttl('key'), None)

    def test_pttl(self):
        expiration_ms = 3000
        self.redis.set('key', 'key')
        self.redis.pexpire('key', expiration_ms)

        result = self.redis.pttl('key')
        # should be an int
        ok_(isinstance(result, int))
        # should be less than the timeout originally set
        ok_(result <= expiration_ms)

    def test_pttl_when_key_absent(self):
        """Test whether, like the redis-py lib, pttl returns None if the key is absent"""

        eq_(self.redis.pttl('invalid_key'), None)

    def test_pttl_no_timeout(self):
        """
        Test whether, like the redis-py lib, pttl returns None if the key has no timeout set.
        """
        self.redis.set('key', 'key')
        eq_(self.redis.pttl('key'), None)

    def test_expireat_calculates_time(self):
        """
        test whether expireat sets the correct ttl, setting a timestamp 30s in the future
        """
        self.redis.set('key', 'key')
        self.redis.expireat('key', int(time()) + 30)

        result = self.redis.ttl('key')
        # should be an int
        ok_(isinstance(result, int))
        # should be less than the timeout originally set
        ok_(result <= 30)

    def test_keys(self):
        eq_([], self.redis.keys("*"))

        self.redis.set("foo", "bar")
        eq_(["foo"], self.redis.keys("*"))
        eq_(["foo"], self.redis.keys("foo*"))
        eq_(["foo"], self.redis.keys("foo"))
        eq_([], self.redis.keys("bar"))

        self.redis.set("food", "bbq")
        eq_({"foo", "food"}, set(self.redis.keys("*")))
        eq_({"foo", "food"}, set(self.redis.keys("foo*")))
        eq_(["foo"], self.redis.keys("foo"))
        eq_(["food"], self.redis.keys("food"))
        eq_([], self.redis.keys("bar"))

    def test_contains(self):
        ok_("foo" not in self.redis)
        self.redis.set("foo", "bar")
        ok_("foo" in self.redis)

    def test_getitem(self):
        with assert_raises(KeyError):
            self.redis["foo"]
        self.redis.set("foo", "bar")
        eq_("bar", self.redis["foo"])
        self.redis.delete("foo")
        with assert_raises(KeyError):
            self.redis["foo"]

    def test_setitem(self):
        eq_(None, self.redis.get("foo"))
        self.redis["foo"] = "bar"
        eq_("bar", self.redis.get("foo"))

    def test_delitem(self):
        self.redis["foo"] = "bar"
        eq_("bar", self.redis["foo"])
        del self.redis["foo"]
        eq_(None, self.redis.get("foo"))
        # redispy does not correctly raise KeyError here, so we don't either
        del self.redis["foo"]
