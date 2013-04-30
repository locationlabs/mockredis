from unittest import TestCase
from mockredis.redis import MockRedis


class TestScript(TestCase):
    """
    Tests for MockRedis scripting operations
    """

    def setUp(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_initially_empty(self):
        """
        List is created empty.
        """
        self.assertEqual(0, len(self.redis.redis["test_list"]))

    def test_register_script_lpush(self):
        # lpush two values
        script_content = "redis.call('LPUSH', KEYS[1], ARGV[1], ARGV[2])"
        script = self.redis.register_script(script_content)
        script(keys=["test_list"], args=["val1", "val2"])

        # validate insertion
        self.assertEquals("list", self.redis.type("test_list"))
        self.assertEquals(["val2", "val1"], self.redis.redis["test_list"])

    def test_register_script_lpop(self):
        self.redis.redis["test_list"] = ["val1", "val2"]
        # lpop one value
        script_content = "return redis.call('LPOP', KEYS[1])"
        script = self.redis.register_script(script_content)
        list_item = script(keys=["test_list"])

        # validate lpop
        self.assertEquals("val1", list_item)
        self.assertEquals(["val2"], self.redis.redis["test_list"])

    def test_register_script_rpoplpush(self):
        self.redis.redis["source_list"] = ["val1", "val2"]
        self.redis.redis["dest_list"] = ["vala", "valb"]
        script_content = "redis.call('RPOPLPUSH', KEYS[1], KEYS[2])"
        script = self.redis.register_script(script_content)
        script(keys=["source_list", "dest_list"])
        self.assertEqual(["val1"], self.redis.redis["source_list"])
        self.assertEqual(["val2", "vala", "valb"], self.redis.redis["dest_list"])

    def test_register_script_rpop_lpush(self):
        self.redis.redis["source_list"] = ["val1", "val2"]
        self.redis.redis["dest_list"] = ["vala", "valb"]
        script_content = """
        local tmp_item = redis.call('RPOP', KEYS[1])
        redis.call('LPUSH', KEYS[2], tmp_item)
        """
        script = self.redis.register_script(script_content)
        script(keys=["source_list", "dest_list"])
        self.assertEqual(["val1"], self.redis.redis["source_list"])
        self.assertEqual(["val2", "vala", "valb"], self.redis.redis["dest_list"])

    def test_eval_lpush(self):
        # lpush two values
        script_content = "redis.call('LPUSH', KEYS[1], ARGV[1], ARGV[2])"
        self.redis.eval(script_content, 1, "test_list", "val1", "val2")

        # validate insertion
        self.assertEquals("list", self.redis.type("test_list"))
        self.assertEquals(["val2", "val1"], self.redis.redis["test_list"])

    def test_eval_lpop(self):
        self.redis.redis["test_list"] = ["val1", "val2"]
        # lpop one value
        script_content = "return redis.call('LPOP', KEYS[1])"
        list_item = self.redis.eval(script_content, 1, "test_list")

        # validate lpop
        self.assertEquals("val1", list_item)
        self.assertEquals(["val2"], self.redis.redis["test_list"])
