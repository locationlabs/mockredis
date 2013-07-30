from unittest import TestCase, skipUnless
from hashlib import sha1
from mockredis.exceptions import RedisError
from mockredis.redis import MockRedis
from mockredis.tests.test_constants import (
    LIST1, LIST2,
    SET1,
    VAL1, VAL2, VAL3, VAL4,
    LPOP_SCRIPT
)


def has_lua():
    """
    Test that lua is available.
    """
    try:
        import lua  # noqa
        return True
    except ImportError:
        return False


@skipUnless(has_lua(), "mockredispy was not installed with lua support")
class TestScript(TestCase):
    """
    Tests for MockRedis scripting operations
    """

    def setUp(self):
        self.redis = MockRedis()
        self.LPOP_SCRIPT_SHA = sha1(LPOP_SCRIPT).hexdigest()

    def test_register_script_lpush(self):
        # lpush two values
        script_content = "redis.call('LPUSH', KEYS[1], ARGV[1], ARGV[2])"
        script = self.redis.register_script(script_content)
        script(keys=[LIST1], args=[VAL1, VAL2])

        # validate insertion
        self.assertEquals([VAL2, VAL1], self.redis.lrange(LIST1, 0, -1))

    def test_register_script_lpop(self):
        self.redis.lpush(LIST1, VAL2, VAL1)

        # lpop one value
        script_content = "return redis.call('LPOP', KEYS[1])"
        script = self.redis.register_script(script_content)
        list_item = script(keys=[LIST1])

        # validate lpop
        self.assertEquals(VAL1, list_item)
        self.assertEquals([VAL2], self.redis.lrange(LIST1, 0, -1))

    def test_register_script_rpoplpush(self):
        self.redis.lpush(LIST1, VAL2, VAL1)
        self.redis.lpush(LIST2, VAL4, VAL3)

        # rpoplpush
        script_content = "redis.call('RPOPLPUSH', KEYS[1], KEYS[2])"
        script = self.redis.register_script(script_content)
        script(keys=[LIST1, LIST2])

        #validate rpoplpush
        self.assertEqual([VAL1], self.redis.lrange(LIST1, 0, -1))
        self.assertEqual([VAL2, VAL3, VAL4], self.redis.lrange(LIST2, 0, -1))

    def test_register_script_rpop_lpush(self):
        self.redis.lpush(LIST1, VAL2, VAL1)
        self.redis.lpush(LIST2, VAL4, VAL3)

        # rpop from LIST1 and lpush the same value to LIST2
        script_content = """
        local tmp_item = redis.call('RPOP', KEYS[1])
        redis.call('LPUSH', KEYS[2], tmp_item)
        """
        script = self.redis.register_script(script_content)
        script(keys=[LIST1, LIST2])

        #validate rpop and then lpush
        self.assertEqual([VAL1], self.redis.lrange(LIST1, 0, -1))
        self.assertEqual([VAL2, VAL3, VAL4], self.redis.lrange(LIST2, 0, -1))

    def test_register_script_client(self):
        # lpush two values in LIST1 in first instance of redis
        self.redis.lpush(LIST1, VAL2, VAL1)

        # create script on first instance of redis
        script_content = LPOP_SCRIPT
        script = self.redis.register_script(script_content)

        # lpush two values in LIST1 in redis2 (second instance of redis)
        redis2 = MockRedis()
        redis2.lpush(LIST1, VAL4, VAL3)

        # execute LPOP script on redis2 instance
        list_item = script(keys=[LIST1], client=redis2)

        # validate lpop from LIST1 in redis2
        self.assertEquals(VAL3, list_item)
        self.assertEquals([VAL4], redis2.lrange(LIST1, 0, -1))
        self.assertEquals([VAL1, VAL2], self.redis.lrange(LIST1, 0, -1))

    def test_eval_lpush(self):
        # lpush two values
        script_content = "redis.call('LPUSH', KEYS[1], ARGV[1], ARGV[2])"
        self.redis.eval(script_content, 1, LIST1, VAL1, VAL2)

        # validate insertion
        self.assertEquals([VAL2, VAL1], self.redis.lrange(LIST1, 0, -1))

    def test_eval_lpop(self):
        self.redis.lpush(LIST1, VAL2, VAL1)

        # lpop one value
        script_content = "return redis.call('LPOP', KEYS[1])"
        list_item = self.redis.eval(script_content, 1, LIST1)

        # validate lpop
        self.assertEquals(VAL1, list_item)
        self.assertEquals([VAL2], self.redis.lrange(LIST1, 0, -1))

    def test_eval_zadd(self):
        # The score and member are reversed when the client is not strict.
        self.redis.strict = False
        script_content = "return redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])"
        self.redis.eval(script_content, 1, SET1, 42, VAL1)

        self.assertEquals(42, self.redis.zrank(SET1, VAL1))

    def test_eval_zrangebyscore(self):
        self.redis.strict = False
        self.redis.zadd(SET1, VAL1, 1)
        self.redis.zadd(SET1, VAL2, 2)

        script = ("return redis.call('zrangebyscore', "
                  "KEYS[1], ARGV[1], ARGV[2], 'LIMIT', 0, 2)")

        self.assertListEquals([],     self.redis.eval(script, 1, SET1, 0, 0))
        self.assertListEquals([1],    self.redis.eval(script, 1, SET1, 0, 1))
        self.assertListEquals([1, 2], self.redis.eval(script, 1, SET1, 0, 2))
        self.assertListEquals([2],    self.redis.eval(script, 1, SET1, 2, 2))

    def test_evalsha(self):
        self.redis.lpush(LIST1, VAL1)
        script = LPOP_SCRIPT
        sha = self.LPOP_SCRIPT_SHA

        # validator error when script not registered
        with self.assertRaises(RedisError) as redisError:
            self.redis.evalsha(self.LPOP_SCRIPT_SHA, 1, LIST1)

        self.assertEqual("Sha not registered", str(redisError.exception))

        self.assertRaises(RedisError, self.redis.evalsha, self.LPOP_SCRIPT_SHA, 1, LIST1)

        # load script and then evalsha
        self.assertEquals(sha, self.redis.script_load(script))
        self.assertEquals(VAL1, self.redis.evalsha(sha, 1, LIST1))
        self.assertEquals(0, self.redis.llen(LIST1))

    def test_script_exists(self):
        script = LPOP_SCRIPT
        sha = self.LPOP_SCRIPT_SHA
        self.assertEquals([False], self.redis.script_exists(sha))
        self.redis.register_script(script)
        self.assertEquals([True], self.redis.script_exists(sha))

    def test_script_flush(self):
        script = LPOP_SCRIPT
        sha = self.LPOP_SCRIPT_SHA
        self.redis.register_script(script)
        self.assertEquals([True], self.redis.script_exists(sha))
        self.redis.script_flush()
        self.assertEquals([False], self.redis.script_exists(sha))

    def test_script_load(self):
        script = LPOP_SCRIPT
        sha = self.LPOP_SCRIPT_SHA
        self.assertEquals([False], self.redis.script_exists(sha))
        self.assertEquals(sha, self.redis.script_load(script))
        self.assertEquals([True], self.redis.script_exists(sha))
