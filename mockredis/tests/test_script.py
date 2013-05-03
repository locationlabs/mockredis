from unittest import TestCase
from mockredis.exceptions import RedisError
from mockredis.redis import MockRedis
from mockredis.sha import Sha
from mockredis.tests.test_constants import (
    LIST1, LIST2,
    VAL1, VAL2, VAL3, VAL4,
    LPOP_SCRIPT
)


class TestScript(TestCase):
    """
    Tests for MockRedis scripting operations
    """

    def setUp(self):
        self.redis = MockRedis()

    def test_initially_empty(self):
        """
        List is created empty.
        """
        self.assertEqual(0, len(self.redis.redis[LIST1]))

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

    def test_eval_lpush(self):
        # lpush two values
        script_content = "redis.call('LPUSH', KEYS[1], ARGV[1], ARGV[2])"
        self.redis.eval(script_content, 1, LIST1, VAL1, VAL2)

        # validate insertion
        self.assertEquals("list", self.redis.type(LIST1))
        self.assertEquals([VAL2, VAL1], self.redis.lrange(LIST1, 0, -1))

    def test_eval_lpop(self):
        self.redis.lpush(LIST1, VAL2, VAL1)

        # lpop one value
        script_content = "return redis.call('LPOP', KEYS[1])"
        list_item = self.redis.eval(script_content, 1, LIST1)

        # validate lpop
        self.assertEquals(VAL1, list_item)
        self.assertEquals([VAL2], self.redis.lrange(LIST1, 0, -1))

    def test_evalsha(self):
        self.redis.lpush(LIST1, VAL1)
        script = LPOP_SCRIPT
        sha = Sha(script)

        # validator error when script not registered
        self.assertRaises(RedisError, self.redis.evalsha, sha, 1, LIST1)

        # load script and then evalsha
        self.assertEquals(sha, self.redis.script_load(script))
        self.assertEquals(VAL1, self.redis.evalsha(sha, 1, LIST1))
        self.assertEquals(0, self.redis.llen(LIST1))

    def test_script_exists(self):
        script = LPOP_SCRIPT
        sha = Sha(script)
        self.assertEquals([False], self.redis.script_exists(sha))
        self.redis.register_script(script)
        self.assertEquals([True], self.redis.script_exists(sha))

    def test_script_flush(self):
        script = LPOP_SCRIPT
        sha = Sha(script)
        self.redis.register_script(script)
        self.assertEquals([True], self.redis.script_exists(sha))
        self.redis.script_flush()
        self.assertEquals([False], self.redis.script_exists(sha))

    def test_script_load(self):
        script = LPOP_SCRIPT
        sha = Sha(script)
        self.assertEquals([False], self.redis.script_exists(sha))
        self.assertEquals(sha, self.redis.script_load(script))
        self.assertEquals([True], self.redis.script_exists(sha))
