from nose.tools import eq_, ok_

from mockredis import MockRedis


class TestRedisHash(object):
    """hash tests"""

    def setup(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_hexists(self):
        hashkey = "hash"
        ok_(not self.redis.hexists(hashkey, "key"))
        self.redis.hset(hashkey, "key", "value")
        ok_(self.redis.hexists(hashkey, "key"))
        ok_(not self.redis.hexists(hashkey, "key2"))

    def test_hgetall(self):
        hashkey = "hash"
        eq_({}, self.redis.hgetall(hashkey))
        self.redis.hset(hashkey, "key", "value")
        eq_({"key": "value"}, self.redis.hgetall(hashkey))

    def test_hdel(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {1: 1, 2: 2, 3: 3})
        eq_(0, self.redis.hdel(hashkey, "foo"))
        eq_({"1": "1", "2": "2", "3": "3"}, self.redis.hgetall(hashkey))
        eq_(2, self.redis.hdel(hashkey, "1", 2))
        eq_({"3": "3"}, self.redis.hgetall(hashkey))
        eq_(1, self.redis.hdel(hashkey, "3", 4))
        eq_({}, self.redis.hgetall(hashkey))
        ok_(not self.redis.exists(hashkey))

    def test_hlen(self):
        hashkey = "hash"
        eq_(0, self.redis.hlen(hashkey))
        self.redis.hset(hashkey, "key", "value")
        eq_(1, self.redis.hlen(hashkey))

    def test_hset(self):
        hashkey = "hash"
        self.redis.hset(hashkey, "key", "value")
        eq_("value", self.redis.hget(hashkey, "key"))

    def test_hget(self):
        hashkey = "hash"
        eq_(None, self.redis.hget(hashkey, "key"))

    def test_hset_integral(self):
        hashkey = "hash"
        self.redis.hset(hashkey, 1, 2)
        eq_("2", self.redis.hget(hashkey, 1))
        eq_("2", self.redis.hget(hashkey, "1"))

    def test_hsetnx(self):
        hashkey = "hash"
        eq_(1, self.redis.hsetnx(hashkey, "key", "value1"))
        eq_("value1", self.redis.hget(hashkey, "key"))
        eq_(0, self.redis.hsetnx(hashkey, "key", "value2"))
        eq_("value1", self.redis.hget(hashkey, "key"))

    def test_hmset(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {"key1": "value1", "key2": "value2"})
        eq_("value1", self.redis.hget(hashkey, "key1"))
        eq_("value2", self.redis.hget(hashkey, "key2"))

    def test_hmset_integral(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {1: 2, 3: 4})
        eq_("2", self.redis.hget(hashkey, "1"))
        eq_("2", self.redis.hget(hashkey, 1))
        eq_("4", self.redis.hget(hashkey, "3"))
        eq_("4", self.redis.hget(hashkey, 3))

    def test_hmget(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {1: 2, 3: 4})
        eq_(["2", None, "4"], self.redis.hmget(hashkey, "1", "2", "3"))
        eq_(["2", None, "4"], self.redis.hmget(hashkey, ["1", "2", "3"]))
        eq_(["2", None, "4"], self.redis.hmget(hashkey, [1, 2, 3]))

    def test_hincrby(self):
        hashkey = "hash"
        eq_(1, self.redis.hincrby(hashkey, "key", 1))
        eq_(3, self.redis.hincrby(hashkey, "key", 2))
        eq_("3", self.redis.hget(hashkey, "key"))

    def test_hincrbyfloat(self):
        hashkey = "hash"
        eq_(1.2, self.redis.hincrbyfloat(hashkey, "key", 1.2))
        eq_(3.5, self.redis.hincrbyfloat(hashkey, "key", 2.3))
        eq_("3.5", self.redis.hget(hashkey, "key"))

    def test_hkeys(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {1: 2, 3: 4})
        eq_(["1", "3"], sorted(self.redis.hkeys(hashkey)))

    def test_hvals(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {1: 2, 3: 4})
        eq_(["2", "4"], sorted(self.redis.hvals(hashkey)))
