from unittest import TestCase
from mockredis import MockRedis


class TestRedisSet(TestCase):
    """set tests"""

    def setUp(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_sadd(self):
        key = "set"
        values = ["one", "uno", "two", "three"]
        for value in values:
            self.assertEquals(1, self.redis.sadd(key, value))

    def test_sadd_multiple(self):
        key = "set"
        values = ["one", "uno", "two", "three"]
        self.assertEquals(4, self.redis.sadd(key, *values))

    def test_sadd_duplicate_key(self):
        key = "set"
        self.assertEquals(1, self.redis.sadd(key, "one"))
        self.assertEquals(0, self.redis.sadd(key, "one"))

    def test_scard(self):
        key = "set"
        self.assertEquals(0, self.redis.scard(key))
        self.assertFalse(key in self.redis.redis)
        values = ["one", "uno", "two", "three"]
        self.assertEquals(4, self.redis.sadd(key, *values))
        self.assertEquals(4, self.redis.scard(key))

    def test_sdiff(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with self.assertRaises(Exception):
            self.redis.sdiff([])

        self.assertEquals(set(), self.redis.sdiff("w"))
        self.assertEquals(set(["one", "two", "three"]), self.redis.sdiff("x"))
        self.assertEquals(set(["two", "three"]), self.redis.sdiff("x", "y"))
        self.assertEquals(set(["two", "three"]), self.redis.sdiff(["x", "y"]))
        self.assertEquals(set(["three"]), self.redis.sdiff("x", "y", "z"))
        self.assertEquals(set(["three"]), self.redis.sdiff(["x", "y"], "z"))

    def test_sdiffstore(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with self.assertRaises(Exception):
            self.redis.sdiffstore("w", [])

        self.assertEquals(3, self.redis.sdiffstore("w", "x"))
        self.assertEquals(set(["one", "two", "three"]), self.redis.smembers("w"))

        self.assertEquals(2, self.redis.sdiffstore("w", "x", "y"))
        self.assertEquals(set(["two", "three"]), self.redis.smembers("w"))
        self.assertEquals(2, self.redis.sdiffstore("w", ["x", "y"]))
        self.assertEquals(set(["two", "three"]), self.redis.smembers("w"))
        self.assertEquals(1, self.redis.sdiffstore("w", "x", "y", "z"))
        self.assertEquals(set(["three"]), self.redis.smembers("w"))
        self.assertEquals(1, self.redis.sdiffstore("w", ["x", "y"], "z"))
        self.assertEquals(set(["three"]), self.redis.smembers("w"))

    def test_sinter(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with self.assertRaises(Exception):
            self.redis.sinter([])

        self.assertEquals(set(), self.redis.sinter("w"))
        self.assertEquals(set(["one", "two", "three"]), self.redis.sinter("x"))
        self.assertEquals(set(["one"]), self.redis.sinter("x", "y"))
        self.assertEquals(set(["two"]), self.redis.sinter(["x", "z"]))
        self.assertEquals(set(), self.redis.sinter("x", "y", "z"))
        self.assertEquals(set(), self.redis.sinter(["x", "y"], "z"))

    def test_sinterstore(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with self.assertRaises(Exception):
            self.redis.sinterstore("w", [])

        self.assertEquals(3, self.redis.sinterstore("w", "x"))
        self.assertEquals(set(["one", "two", "three"]), self.redis.smembers("w"))

        self.assertEquals(1, self.redis.sinterstore("w", "x", "y"))
        self.assertEquals(set(["one"]), self.redis.smembers("w"))
        self.assertEquals(1, self.redis.sinterstore("w", ["x", "z"]))
        self.assertEquals(set(["two"]), self.redis.smembers("w"))
        self.assertEquals(0, self.redis.sinterstore("w", "x", "y", "z"))
        self.assertEquals(set(), self.redis.smembers("w"))
        self.assertEquals(0, self.redis.sinterstore("w", ["x", "y"], "z"))
        self.assertEquals(set(), self.redis.smembers("w"))

    def test_sismember(self):
        key = "set"
        self.assertFalse(self.redis.sismember(key, "one"))
        self.assertFalse(key in self.redis.redis)
        self.assertEquals(1, self.redis.sadd(key, "one"))
        self.assertTrue(self.redis.sismember(key, "one"))
        self.assertFalse(self.redis.sismember(key, "two"))

    def test_ismember_numeric(self):
        """
        Verify string conversion.
        """
        key = "set"
        self.assertEquals(1, self.redis.sadd(key,  1))
        self.assertEquals(set(["1"]), self.redis.smembers(key))
        self.assertTrue(self.redis.sismember(key, "1"))
        self.assertTrue(self.redis.sismember(key, 1))

    def test_smembers(self):
        key = "set"
        self.assertEquals(set(), self.redis.smembers(key))
        self.assertFalse(key in self.redis.redis)
        self.assertEquals(1, self.redis.sadd(key, "one"))
        self.assertEquals(set(["one"]), self.redis.smembers(key))
        self.assertEquals(1, self.redis.sadd(key, "two"))
        self.assertEquals(set(["one", "two"]), self.redis.smembers(key))

    def test_smove(self):
        self.assertEquals(0, self.redis.smove("x", "y", "one"))

        self.assertEquals(2, self.redis.sadd("x", "one", "two"))
        self.assertEquals(set(["one", "two"]), self.redis.smembers("x"))
        self.assertEquals(set(), self.redis.smembers("y"))

        self.assertEquals(0, self.redis.smove("x", "y", "three"))
        self.assertEquals(set(["one", "two"]), self.redis.smembers("x"))
        self.assertEquals(set(), self.redis.smembers("y"))

        self.assertEquals(1, self.redis.smove("x", "y", "one"))
        self.assertEquals(set(["two"]), self.redis.smembers("x"))
        self.assertEquals(set(["one"]), self.redis.smembers("y"))

    def test_spop(self):
        key = "set"
        self.assertEquals(None, self.redis.spop(key))
        self.assertEquals(1, self.redis.sadd(key, "one"))
        self.assertEquals("one", self.redis.spop(key))
        self.assertEquals(0, self.redis.scard(key))
        self.assertEquals(1, self.redis.sadd(key, "one"))
        self.assertEquals(1, self.redis.sadd(key, "two"))
        first = self.redis.spop(key)
        self.assertTrue(first in ["one", "two"])
        self.assertEquals(1, self.redis.scard(key))
        second = self.redis.spop(key)
        self.assertEquals("one" if first == "two" else "two", second)
        self.assertEquals(0, self.redis.scard(key))

    def test_srandmember(self):
        key = "set"
        # count is None
        self.assertEquals(None, self.redis.srandmember(key))
        self.assertEquals(1, self.redis.sadd(key, "one"))
        self.assertEquals("one", self.redis.srandmember(key))
        self.assertEquals(1, self.redis.scard(key))
        self.assertEquals(1, self.redis.sadd(key, "two"))
        self.assertTrue(self.redis.srandmember(key) in ["one", "two"])
        self.assertEquals(2, self.redis.scard(key))
        # count > 0
        self.assertEquals([], self.redis.srandmember("empty", 1))
        self.assertTrue(self.redis.srandmember(key, 1)[0] in ["one", "two"])
        self.assertEquals(set(["one", "two"]), set(self.redis.srandmember(key, 2)))
        # count < 0
        self.assertEquals([], self.redis.srandmember("empty", -1))
        self.assertTrue(self.redis.srandmember(key, -1)[0] in ["one", "two"])
        members = self.redis.srandmember(key, -2)
        self.assertEquals(2, len(members))
        for member in members:
            self.assertTrue(member in ["one", "two"])

    def test_srem(self):
        key = "set"
        self.assertEquals(0, self.redis.srem(key, "one"))
        self.assertEquals(3, self.redis.sadd(key, "one", "two", "three"))
        self.assertEquals(0, self.redis.srem(key, "four"))
        self.assertEquals(2, self.redis.srem(key, "one", "three"))
        self.assertEquals(1, self.redis.srem(key, "two", "four"))

    def test_sunion(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with self.assertRaises(Exception):
            self.redis.sunion([])

        self.assertEquals(set(), self.redis.sunion("v"))
        self.assertEquals(set(["one", "two", "three"]), self.redis.sunion("x"))
        self.assertEquals(set(["one"]), self.redis.sunion("v", "y"))
        self.assertEquals(set(["one", "two"]), self.redis.sunion(["y", "z"]))
        self.assertEquals(set(["one", "two", "three"]), self.redis.sunion("x", "y", "z"))
        self.assertEquals(set(["one", "two", "three"]), self.redis.sunion(["x", "y"], "z"))

    def test_sunionstore(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with self.assertRaises(Exception):
            self.redis.sunionstore("w", [])

        self.assertEquals(0, self.redis.sunionstore("w", "v"))
        self.assertEquals(set(), self.redis.smembers("w"))

        self.assertEquals(3, self.redis.sunionstore("w", "x"))
        self.assertEquals(set(["one", "two", "three"]), self.redis.smembers("w"))

        self.assertEquals(1, self.redis.sunionstore("w", "v", "y"))
        self.assertEquals(set(["one"]), self.redis.smembers("w"))

        self.assertEquals(2, self.redis.sunionstore("w", ["y", "z"]))
        self.assertEquals(set(["one", "two"]), self.redis.smembers("w"))

        self.assertEquals(3, self.redis.sunionstore("w", "x", "y", "z"))
        self.assertEquals(set(["one", "two", "three"]), self.redis.smembers("w"))

        self.assertEquals(3, self.redis.sunionstore("w", ["x", "y"], "z"))
        self.assertEquals(set(["one", "two", "three"]), self.redis.smembers("w"))
