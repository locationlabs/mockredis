from datetime import timedelta
from time import time
import sys

from nose.tools import assert_raises, eq_, ok_

from mockredis.tests.fixtures import setup, teardown

if sys.version_info >= (3, 0):
    long = int


class TestRedis(object):

    def setup(self):
        setup(self)

    def teardown(self):
        teardown(self)

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
            eq_(str(value).encode('utf8'),
                self.redis.get('key'))

            self.redis.hset('hkey', 'item', value)
            eq_(str(value).encode('utf8'),
                self.redis.hget('hkey', 'item'))

            self.redis.sadd('skey', value)
            eq_(set([str(value).encode('utf8')]),
                self.redis.smembers('skey'))

            self.redis.flushdb()

    def test_incr(self):
        '''
        incr, hincr when keys exist
        '''

        values = list([
            (1, b'2'),
            ('1', b'2'),
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
        eq_(b'1', self.redis.get('key'))

        self.redis.hincrby('hkey', 'attr')
        eq_(b'1', self.redis.hget('hkey', 'attr'))

        self.redis.decr('dkey')
        eq_(b'-1', self.redis.get('dkey'))

    def test_ttl(self):
        self.redis.set('key', 'key')
        self.redis.expire('key', 30)

        result = self.redis.ttl('key')
        ok_(isinstance(result, long))
        # should be less than the timeout originally set
        ok_(result <= 30)

    def test_ttl_timedelta(self):
        self.redis.set('key', 'key')
        self.redis.expire('key', timedelta(seconds=30))

        result = self.redis.ttl('key')
        ok_(isinstance(result, long))
        # should be less than the timeout originally set
        ok_(result <= 30)

    def test_ttl_when_absent(self):
        """
        Test absent ttl handling.
        """
        eq_(self.redis.ttl("invalid_key"), None)
        self.redis.set("key", "value")
        eq_(self.redis.ttl("key"), None)

        # redis >= 2.8.0 returns -2 if the key does exist
        eq_(self.redis_strict.ttl("invalid_key"), -2)
        # redis >= 2.8.0 returns -1 if there is no ttl
        self.redis_strict.set("key", "value")
        eq_(self.redis_strict.ttl("key"), -1)

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
        ok_(isinstance(result, long))
        # should be less than the timeout originally set
        ok_(result <= expiration_ms)

    def test_pttl_when_absent(self):
        """
        Test absent pttl handling.
        """
        # redis >= 2.8.0 returns -2 if the key does exist
        eq_(self.redis.pttl("invalid_key"), None)
        eq_(self.redis_strict.pttl("invalid_key"), -2)

        # redis >= 2.8.0 returns -1 if there is no ttl
        self.redis.set("key", "value")
        eq_(self.redis.pttl("key"), None)
        self.redis_strict.set("key", "value")
        eq_(self.redis_strict.pttl("key"), -1)

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
        ok_(isinstance(result, long))
        # should be less than the timeout originally set
        ok_(result <= 30, "Expected {} to be less than 30".format(result))

    def test_keys(self):
        eq_([], self.redis.keys("*"))

        self.redis.set("foo", "bar")
        eq_([b"foo"], self.redis.keys("*"))
        eq_([b"foo"], self.redis.keys("foo*"))
        eq_([b"foo"], self.redis.keys("foo"))
        eq_([], self.redis.keys("bar"))

        self.redis.set("food", "bbq")
        eq_({b"foo", b"food"}, set(self.redis.keys("*")))
        eq_({b"foo", b"food"}, set(self.redis.keys("foo*")))
        eq_([b"foo"], self.redis.keys("foo"))
        eq_([b"food"], self.redis.keys("food"))
        eq_([], self.redis.keys("bar"))

    def test_keys_unicode(self):
        eq_([], self.redis.keys("*"))

        # This is a little backwards, but python3.2 has trouble with unicode in strings.
        key_as_utf8 = b'eat \xf0\x9f\x8d\xb0 now'
        key = key_as_utf8.decode('utf-8')
        self.redis.set(key, "bar")
        eq_([key_as_utf8], self.redis.keys("*"))
        eq_([key_as_utf8], self.redis.keys("eat*"))
        eq_([key_as_utf8], self.redis.keys("[ea]at * n?[a-z]"))

        unicode_prefix = b'eat \xf0\x9f\x8d\xb0*'.decode('utf-8')
        eq_([key_as_utf8], self.redis.keys(unicode_prefix))
        eq_([key_as_utf8], self.redis.keys(unicode_prefix.encode('utf-8')))
        unicode_prefix = b'eat \xf0\x9f\x8d\xb1*'.decode('utf-8')
        eq_([], self.redis.keys(unicode_prefix))

    def test_contains(self):
        ok_("foo" not in self.redis)
        self.redis.set("foo", "bar")
        ok_("foo" in self.redis)

    def test_getitem(self):
        with assert_raises(KeyError):
            self.redis["foo"]
        self.redis.set("foo", "bar")
        eq_(b"bar", self.redis["foo"])
        self.redis.delete("foo")
        with assert_raises(KeyError):
            self.redis["foo"]

    def test_setitem(self):
        eq_(None, self.redis.get("foo"))
        self.redis["foo"] = "bar"
        eq_(b"bar", self.redis.get("foo"))

    def test_delitem(self):
        self.redis["foo"] = "bar"
        eq_(b"bar", self.redis["foo"])
        del self.redis["foo"]
        eq_(None, self.redis.get("foo"))
        # redispy does not correctly raise KeyError here, so we don't either
        del self.redis["foo"]

    def test_rename(self):
        self.redis["foo"] = "bar"
        ok_(self.redis.rename("foo", "new_foo"))
        eq_(b"bar", self.redis.get("new_foo"))

    def test_renamenx(self):
        self.redis["foo"] = "bar"
        self.redis["foo2"] = "bar2"
        eq_(self.redis.renamenx("foo", "foo2"), 0)
        eq_(b"bar2", self.redis.get("foo2"))
        eq_(self.redis.renamenx("foo", "foo3"), 1)
        eq_(b"bar", self.redis.get("foo3"))

    def test_dbsize(self):
        self.redis["foo"] = "bar"
        eq_(1, self.redis.dbsize())
        del self.redis["foo"]
        eq_(0, self.redis.dbsize())
