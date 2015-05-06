from nose.tools import assert_raises, eq_, ok_

from mockredis.exceptions import ResponseError
from mockredis.tests.fixtures import setup, teardown


class TestRedisSet(object):
    """set tests"""

    def setup(self):
        setup(self)

    def teardown(self):
        teardown(self)

    def test_sadd_empty(self):
        key = "set"
        values = []
        with assert_raises(ResponseError):
            self.redis.sadd(key, *values)

    def test_sadd(self):
        key = "set"
        values = ["one", "uno", "two", "three"]
        for value in values:
            eq_(1, self.redis.sadd(key, value))

    def test_sadd_multiple(self):
        key = "set"
        values = ["one", "uno", "two", "three"]
        eq_(4, self.redis.sadd(key, *values))

    def test_sadd_duplicate_key(self):
        key = "set"
        eq_(1, self.redis.sadd(key, "one"))
        eq_(0, self.redis.sadd(key, "one"))

    def test_scard(self):
        key = "set"
        eq_(0, self.redis.scard(key))
        ok_(key not in self.redis)
        values = ["one", "uno", "two", "three"]
        eq_(4, self.redis.sadd(key, *values))
        eq_(4, self.redis.scard(key))

    def test_sdiff(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with assert_raises(Exception):
            self.redis.sdiff([])

        eq_(set(), self.redis.sdiff("w"))
        eq_(set([b"one", b"two", b"three"]), self.redis.sdiff("x"))
        eq_(set([b"two", b"three"]), self.redis.sdiff("x", "y"))
        eq_(set([b"two", b"three"]), self.redis.sdiff(["x", "y"]))
        eq_(set([b"three"]), self.redis.sdiff("x", "y", "z"))
        eq_(set([b"three"]), self.redis.sdiff(["x", "y"], "z"))

    def test_sdiffstore(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with assert_raises(Exception):
            self.redis.sdiffstore("w", [])

        eq_(3, self.redis.sdiffstore("w", "x"))
        eq_(set([b"one", b"two", b"three"]), self.redis.smembers("w"))

        eq_(2, self.redis.sdiffstore("w", "x", "y"))
        eq_(set([b"two", b"three"]), self.redis.smembers("w"))
        eq_(2, self.redis.sdiffstore("w", ["x", "y"]))
        eq_(set([b"two", b"three"]), self.redis.smembers("w"))
        eq_(1, self.redis.sdiffstore("w", "x", "y", "z"))
        eq_(set([b"three"]), self.redis.smembers("w"))
        eq_(1, self.redis.sdiffstore("w", ["x", "y"], "z"))
        eq_(set([b"three"]), self.redis.smembers("w"))

    def test_sinter(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with assert_raises(Exception):
            self.redis.sinter([])

        eq_(set(), self.redis.sinter("w"))
        eq_(set([b"one", b"two", b"three"]), self.redis.sinter("x"))
        eq_(set([b"one"]), self.redis.sinter("x", "y"))
        eq_(set([b"two"]), self.redis.sinter(["x", "z"]))
        eq_(set(), self.redis.sinter("x", "y", "z"))
        eq_(set(), self.redis.sinter(["x", "y"], "z"))

    def test_sinterstore(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with assert_raises(Exception):
            self.redis.sinterstore("w", [])

        eq_(3, self.redis.sinterstore("w", "x"))
        eq_(set([b"one", b"two", b"three"]), self.redis.smembers("w"))

        eq_(1, self.redis.sinterstore("w", "x", "y"))
        eq_(set([b"one"]), self.redis.smembers("w"))
        eq_(1, self.redis.sinterstore("w", ["x", "z"]))
        eq_(set([b"two"]), self.redis.smembers("w"))
        eq_(0, self.redis.sinterstore("w", "x", "y", "z"))
        eq_(set(), self.redis.smembers("w"))
        eq_(0, self.redis.sinterstore("w", ["x", "y"], "z"))
        eq_(set(), self.redis.smembers("w"))

    def test_sismember(self):
        key = "set"
        ok_(not self.redis.sismember(key, "one"))
        ok_(key not in self.redis)

        eq_(1, self.redis.sadd(key, "one"))
        ok_(self.redis.sismember(key, "one"))
        ok_(not self.redis.sismember(key, "two"))
        eq_(0, self.redis.sismember(key, "two"))

    def test_ismember_numeric(self):
        """
        Verify string conversion.
        """
        key = "set"
        eq_(1, self.redis.sadd(key,  1))
        eq_(set([b"1"]), self.redis.smembers(key))
        ok_(self.redis.sismember(key, "1"))
        ok_(self.redis.sismember(key, 1))

    def test_smembers(self):
        key = "set"
        eq_(set(), self.redis.smembers(key))
        ok_(key not in self.redis)
        eq_(1, self.redis.sadd(key, "one"))
        eq_(set([b"one"]), self.redis.smembers(key))
        eq_(1, self.redis.sadd(key, "two"))
        eq_(set([b"one", b"two"]), self.redis.smembers(key))

    def test_smembers_copy(self):
        key = "set"
        self.redis.sadd(key, "one", "two", "three")
        members = self.redis.smembers(key)
        eq_({b"one", b"two", b"three"}, members)
        for member in members:
            # Checking that SMEMBERS returns the copy of internal data structure instead of
            # direct references. Otherwise SREM operation may give following error.
            # RuntimeError: Set changed size during iteration
            self.redis.srem(key, member)
        eq_(set(), self.redis.smembers(key))

    def test_smove(self):
        eq_(0, self.redis.smove("x", "y", "one"))

        eq_(2, self.redis.sadd("x", "one", "two"))
        eq_(set([b"one", b"two"]), self.redis.smembers("x"))
        eq_(set(), self.redis.smembers("y"))

        eq_(0, self.redis.smove("x", "y", "three"))
        eq_(set([b"one", b"two"]), self.redis.smembers("x"))
        eq_(set(), self.redis.smembers("y"))

        eq_(1, self.redis.smove("x", "y", "one"))
        eq_(set([b"two"]), self.redis.smembers("x"))
        eq_(set([b"one"]), self.redis.smembers("y"))

    def test_spop(self):
        key = "set"
        eq_(None, self.redis.spop(key))
        eq_(1, self.redis.sadd(key, "one"))
        eq_(b"one", self.redis.spop(key))
        eq_(0, self.redis.scard(key))
        eq_(1, self.redis.sadd(key, "one"))
        eq_(1, self.redis.sadd(key, "two"))
        first = self.redis.spop(key)
        ok_(first in [b"one", b"two"])
        eq_(1, self.redis.scard(key))
        second = self.redis.spop(key)
        eq_(b"one" if first == b"two" else b"two", second)
        eq_(0, self.redis.scard(key))
        eq_([], self.redis.keys("*"))

    def test_srandmember(self):
        key = "set"
        # count is None
        eq_(None, self.redis.srandmember(key))
        eq_(1, self.redis.sadd(key, "one"))
        eq_(b"one", self.redis.srandmember(key))
        eq_(1, self.redis.scard(key))
        eq_(1, self.redis.sadd(key, "two"))
        ok_(self.redis.srandmember(key) in [b"one", b"two"])
        eq_(2, self.redis.scard(key))
        # count > 0
        eq_([], self.redis.srandmember("empty", 1))
        ok_(self.redis.srandmember(key, 1)[0] in [b"one", b"two"])
        eq_(set([b"one", b"two"]), set(self.redis.srandmember(key, 2)))
        # count < 0
        eq_([], self.redis.srandmember("empty", -1))
        ok_(self.redis.srandmember(key, -1)[0] in [b"one", b"two"])
        members = self.redis.srandmember(key, -2)
        eq_(2, len(members))
        for member in members:
            ok_(member in [b"one", b"two"])

    def test_srem(self):
        key = "set"
        eq_(0, self.redis.srem(key, "one"))
        eq_(3, self.redis.sadd(key, "one", "two", "three"))
        eq_(0, self.redis.srem(key, "four"))
        eq_(2, self.redis.srem(key, "one", "three"))
        eq_(1, self.redis.srem(key, "two", "four"))
        eq_([], self.redis.keys("*"))

    def test_sunion(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with assert_raises(Exception):
            self.redis.sunion([])

        eq_(set(), self.redis.sunion("v"))
        eq_(set([b"one", b"two", b"three"]), self.redis.sunion("x"))
        eq_(set([b"one"]), self.redis.sunion("v", "y"))
        eq_(set([b"one", b"two"]), self.redis.sunion(["y", "z"]))
        eq_(set([b"one", b"two", b"three"]), self.redis.sunion("x", "y", "z"))
        eq_(set([b"one", b"two", b"three"]), self.redis.sunion(["x", "y"], "z"))

    def test_sunionstore(self):
        self.redis.sadd("x", "one", "two", "three")
        self.redis.sadd("y", "one")
        self.redis.sadd("z", "two")

        with assert_raises(Exception):
            self.redis.sunionstore("w", [])

        eq_(0, self.redis.sunionstore("w", "v"))
        eq_(set(), self.redis.smembers("w"))

        eq_(3, self.redis.sunionstore("w", "x"))
        eq_(set([b"one", b"two", b"three"]), self.redis.smembers("w"))

        eq_(1, self.redis.sunionstore("w", "v", "y"))
        eq_(set([b"one"]), self.redis.smembers("w"))

        eq_(2, self.redis.sunionstore("w", ["y", "z"]))
        eq_(set([b"one", b"two"]), self.redis.smembers("w"))

        eq_(3, self.redis.sunionstore("w", "x", "y", "z"))
        eq_(set([b"one", b"two", b"three"]), self.redis.smembers("w"))

        eq_(3, self.redis.sunionstore("w", ["x", "y"], "z"))
        eq_(set([b"one", b"two", b"three"]), self.redis.smembers("w"))
