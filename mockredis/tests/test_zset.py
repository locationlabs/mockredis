from nose.tools import assert_raises, eq_, ok_

from mockredis.tests.fixtures import setup


class TestRedisZset(object):
    """zset tests"""

    def setup(self):
        setup(self)

    def test_zadd(self):
        key = "zset"
        values = [("one", 1), ("uno", 1), ("two", 2), ("three", 3)]
        for member, score in values:
            eq_(1, self.redis.zadd(key, member, score))

    def test_zadd_strict(self):
        """Argument order for zadd depends on strictness"""
        key = "zset"
        values = [("one", 1), ("uno", 1), ("two", 2), ("three", 3)]
        for member, score in values:
            eq_(1, self.redis_strict.zadd(key, score, member))

    def test_zadd_duplicate_key(self):
        key = "zset"
        eq_(1, self.redis.zadd(key, "one", 1.0))
        eq_(0, self.redis.zadd(key, "one", 2.0))

    def test_zadd_wrong_type(self):
        key = "zset"
        self.redis.set(key, "value")
        with assert_raises(Exception):
            self.redis.zadd(key, "one", 2.0)

    def test_zadd_multiple_bad_args(self):
        key = "zset"
        args = ["one", 1, "two"]
        with assert_raises(Exception):
            self.redis.zadd(key, *args)

    def test_zadd_multiple_bad_score(self):
        key = "zset"
        with assert_raises(Exception):
            self.redis.zadd(key, "one", "two")

    def test_zadd_multiple_args(self):
        key = "zset"
        args = ["one", 1, "uno", 1, "two", 2, "three", 3]
        eq_(4, self.redis.zadd(key, *args))

    def test_zadd_multiple_kwargs(self):
        key = "zset"
        kwargs = {"one": 1, "uno": 1, "two": 2, "three": 3}
        eq_(4, self.redis.zadd(key, **kwargs))

    def test_zcard(self):
        key = "zset"
        eq_(0, self.redis.zcard(key))
        self.redis.zadd(key, "one", 1)
        eq_(1, self.redis.zcard(key))
        self.redis.zadd(key, "one", 2)
        eq_(1, self.redis.zcard(key))
        self.redis.zadd(key, "two", 2)
        eq_(2, self.redis.zcard(key))

    def test_zincrby(self):
        key = "zset"
        eq_(1.0, self.redis.zincrby(key, "member1"))
        eq_(2.0, self.redis.zincrby(key, "member2", 2))
        eq_(-1.0, self.redis.zincrby(key, "member1", -2))

    def test_zrange(self):
        key = "zset"
        eq_([], self.redis.zrange(key, 0, -1))
        self.redis.zadd(key, "one", 1.5)
        self.redis.zadd(key, "two", 2.5)
        self.redis.zadd(key, "three", 3.5)

        # full range
        eq_(["one", "two", "three"],
            self.redis.zrange(key, 0, -1))
        # withscores
        eq_([("one", 1.5), ("two", 2.5), ("three", 3.5)],
            self.redis.zrange(key, 0, -1, withscores=True))

        with assert_raises(ValueError):
            # invalid literal for int() with base 10
            self.redis.zrange(key, 0, -1, withscores=True, score_cast_func=int)

        # score_cast_func
        def cast_to_int(score):
            return int(float(score))

        eq_([("one", 1), ("two", 2), ("three", 3)],
            self.redis.zrange(key, 0, -1, withscores=True, score_cast_func=cast_to_int))

        # positive ranges
        eq_(["one"], self.redis.zrange(key, 0, 0))
        eq_(["one", "two"], self.redis.zrange(key, 0, 1))
        eq_(["one", "two", "three"], self.redis.zrange(key, 0, 2))
        eq_(["one", "two", "three"], self.redis.zrange(key, 0, 3))
        eq_(["two", "three"], self.redis.zrange(key, 1, 2))
        eq_(["three"], self.redis.zrange(key, 2, 3))

        # negative ends
        eq_(["one", "two", "three"], self.redis.zrange(key, 0, -1))
        eq_(["one", "two"], self.redis.zrange(key, 0, -2))
        eq_(["one"], self.redis.zrange(key, 0, -3))
        eq_([], self.redis.zrange(key, 0, -4))

        # negative starts
        eq_([], self.redis.zrange(key, -1, 0))
        eq_(["three"], self.redis.zrange(key, -1, -1))
        eq_(["two", "three"], self.redis.zrange(key, -2, -1))
        eq_(["one", "two", "three"], self.redis.zrange(key, -3, -1))
        eq_(["one", "two", "three"], self.redis.zrange(key, -4, -1))

        # desc
        eq_(["three", "two", "one"], self.redis.zrange(key, 0, 2, desc=True))
        eq_(["two", "one"], self.redis.zrange(key, 1, 2, desc=True))
        eq_(["three", "two"], self.redis.zrange(key, 0, 1, desc=True))

    def test_zrem(self):
        key = "zset"
        ok_(not self.redis.zrem(key, "two"))

        self.redis.zadd(key, "one", 1.0)
        eq_(1, self.redis.zcard(key))
        eq_(["zset"], self.redis.keys("*"))

        ok_(self.redis.zrem(key, "one"))
        eq_(0, self.redis.zcard(key))
        eq_([], self.redis.keys("*"))

    def test_zscore(self):
        key = "zset"
        eq_(None, self.redis.zscore(key, "one"))

        self.redis.zadd(key, "one", 1.0)
        eq_(1.0, self.redis.zscore(key, "one"))

    def test_zscore_int_member(self):
        key = "zset"
        eq_(None, self.redis.zscore(key, 1))

        self.redis.zadd(key, 1, 1.0)
        eq_(1.0, self.redis.zscore(key, 1))

    def test_zrank(self):
        key = "zset"
        eq_(None, self.redis.zrank(key, "two"))

        self.redis.zadd(key, "one", 1.0)
        self.redis.zadd(key, "two", 2.0)
        eq_(0, self.redis.zrank(key, "one"))
        eq_(1, self.redis.zrank(key, "two"))

    def test_zrank_int_member(self):
        key = "zset"
        eq_(None, self.redis.zrank(key, 2))

        self.redis.zadd(key, "one", 1.0)
        self.redis.zadd(key, 2, 2.0)
        eq_(0, self.redis.zrank(key, "one"))
        eq_(1, self.redis.zrank(key, 2))

    def test_zcount(self):
        key = "zset"
        eq_(0, self.redis.zcount(key, "-inf", "inf"))

        self.redis.zadd(key, "one", 1.0)
        self.redis.zadd(key, "two", 2.0)

        eq_(2, self.redis.zcount(key, "-inf", "inf"))
        eq_(1, self.redis.zcount(key, "-inf", 1.0))
        eq_(1, self.redis.zcount(key, "-inf", 1.5))
        eq_(2, self.redis.zcount(key, "-inf", 2.0))
        eq_(2, self.redis.zcount(key, "-inf", 2.5))
        eq_(1, self.redis.zcount(key, 0.5, 1.0))
        eq_(1, self.redis.zcount(key, 0.5, 1.5))
        eq_(2, self.redis.zcount(key, 0.5, 2.0))
        eq_(2, self.redis.zcount(key, 0.5, 2.5))
        eq_(2, self.redis.zcount(key, 0.5, "inf"))

        eq_(0, self.redis.zcount(key, "inf", "-inf"))
        eq_(0, self.redis.zcount(key, 2.0, 0.5))

    def test_zrangebyscore(self):
        key = "zset"
        eq_([], self.redis.zrangebyscore(key, "-inf", "inf"))
        self.redis.zadd(key, "one", 1.5)
        self.redis.zadd(key, "two", 2.5)
        self.redis.zadd(key, "three", 3.5)

        eq_(["one", "two", "three"],
            self.redis.zrangebyscore(key, "-inf", "inf"))
        eq_([("one", 1.5), ("two", 2.5), ("three", 3.5)],
            self.redis.zrangebyscore(key, "-inf", "inf", withscores=True))

        with assert_raises(ValueError):
            # invalid literal for int() with base 10
            self.redis.zrangebyscore(key,
                                     "-inf",
                                     "inf",
                                     withscores=True,
                                     score_cast_func=int)

        def cast_score(score):
            return int(float(score))

        eq_([("one", 1), ("two", 2), ("three", 3)],
            self.redis.zrangebyscore(key,
                                     "-inf",
                                     "inf",
                                     withscores=True,
                                     score_cast_func=cast_score))

        eq_(["one"],
            self.redis.zrangebyscore(key, 1.0, 2.0))
        eq_(["one", "two"],
            self.redis.zrangebyscore(key, 1.0, 3.0))
        eq_(["one"],
            self.redis.zrangebyscore(key, 1.0, 3.0, start=0, num=1))
        eq_(["two"],
            self.redis.zrangebyscore(key, 1.0, 3.0, start=1, num=1))
        eq_(["two", "three"],
            self.redis.zrangebyscore(key, 1.0, 3.5, start=1, num=4))
        eq_([],
            self.redis.zrangebyscore(key, 1.0, 3.5, start=3, num=4))

    def test_zrevrank(self):
        key = "zset"
        eq_(None, self.redis.zrevrank(key, "two"))

        self.redis.zadd(key, "one", 1.0)
        self.redis.zadd(key, "two", 2.0)
        eq_(1, self.redis.zrevrank(key, "one"))
        eq_(0, self.redis.zrevrank(key, "two"))

    def test_zrevrangebyscore(self):
        key = "zset"
        eq_([], self.redis.zrevrangebyscore(key, "inf", "-inf"))
        self.redis.zadd(key, "one", 1.5)
        self.redis.zadd(key, "two", 2.5)
        self.redis.zadd(key, "three", 3.5)

        eq_(["three", "two", "one"],
            self.redis.zrevrangebyscore(key, "inf", "-inf"))
        eq_([("three", 3.5), ("two", 2.5), ("one", 1.5)],
            self.redis.zrevrangebyscore(key, "inf", "-inf", withscores=True))

        with assert_raises(ValueError):
            # invalid literal for int() with base 10
            self.redis.zrevrangebyscore(key,
                                        "inf",
                                        "-inf",
                                        withscores=True,
                                        score_cast_func=int)

        def cast_score(score):
            return int(float(score))

        eq_([("three", 3), ("two", 2), ("one", 1)],
            self.redis.zrevrangebyscore(key,
                                        "inf",
                                        "-inf",
                                        withscores=True,
                                        score_cast_func=cast_score))

        eq_(["one"],
            self.redis.zrevrangebyscore(key, 2.0, 1.0))
        eq_(["two", "one"],
            self.redis.zrevrangebyscore(key, 3.0, 1.0))
        eq_(["two"],
            self.redis.zrevrangebyscore(key, 3.0, 1.0, start=0, num=1))
        eq_(["one"],
            self.redis.zrevrangebyscore(key, 3.0, 1.0, start=1, num=1))
        eq_(["two", "one"],
            self.redis.zrevrangebyscore(key, 3.5, 1.0, start=1, num=4))
        eq_([],
            self.redis.zrevrangebyscore(key, 3.5, 1.0, start=3, num=4))

    def test_zremrangebyrank(self):
        key = "zset"
        eq_(0, self.redis.zremrangebyrank(key, 0, -1))

        self.redis.zadd(key, "one", 1.0)
        self.redis.zadd(key, "two", 2.0)
        self.redis.zadd(key, "three", 3.0)

        eq_(2, self.redis.zremrangebyrank(key, 0, 1))

        eq_(["three"], self.redis.zrange(key, 0, -1))
        eq_(1, self.redis.zremrangebyrank(key, 0, -1))

        eq_([], self.redis.zrange(key, 0, -1))
        eq_([], self.redis.keys("*"))

    def test_zremrangebyscore(self):
        key = "zset"
        eq_(0, self.redis.zremrangebyscore(key, "-inf", "inf"))

        self.redis.zadd(key, "one", 1.0)
        self.redis.zadd(key, "two", 2.0)
        self.redis.zadd(key, "three", 3.0)

        eq_(1, self.redis.zremrangebyscore(key, 0, 1))

        eq_(["two", "three"], self.redis.zrange(key, 0, -1))
        eq_(2, self.redis.zremrangebyscore(key, 2.0, "inf"))

        eq_([], self.redis.zrange(key, 0, -1))
        eq_([], self.redis.keys("*"))

    def test_zunionstore_no_keys(self):
        key = "zset"

        eq_(0, self.redis.zunionstore(key, ["zset1", "zset2"]))

    def test_zunionstore_default(self):
        # sum is default
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(3, self.redis.zunionstore(key, ["zset1", "zset2"]))
        eq_([("one", 1.0), ("three", 3.0), ("two", 4.5)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zunionstore_sum(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(3, self.redis.zunionstore(key, ["zset1", "zset2"], aggregate="sum"))
        eq_([("one", 1.0), ("three", 3.0), ("two", 4.5)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zunionstore_SUM(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(3, self.redis.zunionstore(key, ["zset1", "zset2"], aggregate="SUM"))
        eq_([("one", 1.0), ("three", 3.0), ("two", 4.5)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zunionstore_min(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(3, self.redis.zunionstore(key, ["zset1", "zset2"], aggregate="min"))
        eq_([("one", 1.0), ("two", 2.0), ("three", 3.0)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zunionstore_MIN(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(3, self.redis.zunionstore(key, ["zset1", "zset2"], aggregate="MIN"))
        eq_([("one", 1.0), ("two", 2.0), ("three", 3.0)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zunionstore_max(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        key = "zset"
        eq_(3, self.redis.zunionstore(key, ["zset1", "zset2"], aggregate="max"))
        eq_([("one", 1.0), ("two", 2.5), ("three", 3.0)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zunionstore_MAX(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        key = "zset"
        eq_(3, self.redis.zunionstore(key, ["zset1", "zset2"], aggregate="MAX"))
        eq_([("one", 1.0), ("two", 2.5), ("three", 3.0)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zinterstore_no_keys(self):
        key = "zset"

        # no keys
        eq_(0, self.redis.zinterstore(key, ["zset1", "zset2"]))

    def test_zinterstore_default(self):
        # sum is default
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(1, self.redis.zinterstore(key, ["zset1", "zset2"]))
        eq_([("two", 4.5)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zinterstore_sum(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(1, self.redis.zinterstore(key, ["zset1", "zset2"], aggregate="sum"))
        eq_([("two", 4.5)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zinterstore_SUM(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(1, self.redis.zinterstore(key, ["zset1", "zset2"], aggregate="SUM"))
        eq_([("two", 4.5)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zinterstore_min(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(1, self.redis.zinterstore(key, ["zset1", "zset2"], aggregate="min"))
        eq_([("two", 2.0)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zinterstore_MIN(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(1, self.redis.zinterstore(key, ["zset1", "zset2"], aggregate="MIN"))
        eq_([("two", 2.0)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zinterstore_max(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(1, self.redis.zinterstore(key, ["zset1", "zset2"], aggregate="max"))
        eq_([("two", 2.5)],
            self.redis.zrange(key, 0, -1, withscores=True))

    def test_zinterstore_MAX(self):
        key = "zset"
        self.redis.zadd("zset1", "one", 1.0)
        self.redis.zadd("zset1", "two", 2.0)
        self.redis.zadd("zset2", "two", 2.5)
        self.redis.zadd("zset2", "three", 3.0)

        eq_(1, self.redis.zinterstore(key, ["zset1", "zset2"], aggregate="MAX"))
        eq_([("two", 2.5)],
            self.redis.zrange(key, 0, -1, withscores=True))
