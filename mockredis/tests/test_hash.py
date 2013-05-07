from unittest import TestCase
from mockredis import MockRedis


class TestRedisHash(TestCase):
    """hash tests"""

    def setUp(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_hexists(self):
        hashkey = "hash"
        self.assertEquals(False, self.redis.hexists(hashkey, "key"))
        self.redis.hset(hashkey, "key", "value")
        self.assertEquals(True, self.redis.hexists(hashkey, "key"))
        self.assertEquals(False, self.redis.hexists(hashkey, "key2"))

    def test_hgetall(self):
        hashkey = "hash"
        self.assertEquals({}, self.redis.hgetall(hashkey))
        self.redis.hset(hashkey, "key", "value")
        self.assertEquals({"key": "value"}, self.redis.hgetall(hashkey))

    def test_hdel(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {1: 1, 2: 2, 3: 3})
        self.assertEquals(0, self.redis.hdel(hashkey, "foo"))
        self.assertEquals({"1": "1", "2": "2", "3": "3"}, self.redis.hgetall(hashkey))
        self.assertEquals(2, self.redis.hdel(hashkey, "1", 2))
        self.assertEquals({"3": "3"}, self.redis.hgetall(hashkey))
        self.assertEquals(1, self.redis.hdel(hashkey, "3", 4))
        self.assertEquals({}, self.redis.hgetall(hashkey))

    def test_hlen(self):
        hashkey = "hash"
        self.assertEquals(0, self.redis.hlen(hashkey))
        self.redis.hset(hashkey, "key", "value")
        self.assertEquals(1, self.redis.hlen(hashkey))

    def test_hset(self):
        hashkey = "hash"
        self.redis.hset(hashkey, "key", "value")
        self.assertEquals("value", self.redis.hget(hashkey, "key"))

    def test_hset_integral(self):
        hashkey = "hash"
        self.redis.hset(hashkey, 1, 2)
        self.assertEquals("2", self.redis.hget(hashkey, 1))
        self.assertEquals("2", self.redis.hget(hashkey, "1"))

    def test_hsetnx(self):
        hashkey = "hash"
        self.assertEquals(1, self.redis.hsetnx(hashkey, "key", "value1"))
        self.assertEquals("value1", self.redis.hget(hashkey, "key"))
        self.assertEquals(0, self.redis.hsetnx(hashkey, "key", "value2"))
        self.assertEquals("value1", self.redis.hget(hashkey, "key"))

    def test_hmset(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {"key1": "value1", "key2": "value2"})
        self.assertEquals("value1", self.redis.hget(hashkey, "key1"))
        self.assertEquals("value2", self.redis.hget(hashkey, "key2"))

    def test_hmset_integral(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {1: 2, 3: 4})
        self.assertEquals("2", self.redis.hget(hashkey, "1"))
        self.assertEquals("2", self.redis.hget(hashkey, 1))
        self.assertEquals("4", self.redis.hget(hashkey, "3"))
        self.assertEquals("4", self.redis.hget(hashkey, 3))

    def test_hmget(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {1: 2, 3: 4})
        self.assertEquals(["2", None, "4"], self.redis.hmget(hashkey, "1", "2", "3"))
        self.assertEquals(["2", None, "4"], self.redis.hmget(hashkey, ["1", "2", "3"]))
        self.assertEquals(["2", None, "4"], self.redis.hmget(hashkey, [1, 2, 3]))

    def test_hincrby(self):
        hashkey = "hash"
        self.assertEquals(1, self.redis.hincrby(hashkey, "key", 1))
        self.assertEquals(3, self.redis.hincrby(hashkey, "key", 2))
        self.assertEquals("3", self.redis.hget(hashkey, "key"))

    def test_hincrbyfloat(self):
        hashkey = "hash"
        self.assertEquals(1.2, self.redis.hincrbyfloat(hashkey, "key", 1.2))
        self.assertEquals(3.5, self.redis.hincrbyfloat(hashkey, "key", 2.3))
        self.assertEquals("3.5", self.redis.hget(hashkey, "key"))

    def test_hkeys(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {1: 2, 3: 4})
        self.assertEquals(["1", "3"], sorted(self.redis.hkeys(hashkey)))

    def test_hvals(self):
        hashkey = "hash"
        self.redis.hmset(hashkey, {1: 2, 3: 4})
        self.assertEquals(["2", "4"], sorted(self.redis.hvals(hashkey)))
