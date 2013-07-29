from unittest import TestCase, skipUnless
from hashlib import sha1
from mockredis.exceptions import RedisError
from mockredis.redis import MockRedis
from mockredis.tests.test_constants import (
    LIST1, LIST2,
    VAL1, VAL2, VAL3, VAL4,
    LPOP_SCRIPT
)
from mockredis.script import Script as MockredisScript


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
        import lua
        self.lua = lua
        self.lua_globals = lua.globals()

        assert_equal_list = """
        function compare_list(list1, list2)
            if #list1 ~= #list2 then
                return false
            end
            for i, item1 in ipairs(list1) do
                if item1 ~= list2[i] then
                    return false
                end
            end
            return true
        end

        function assert_equal_list(list1, list2)
            assert(compare_list(list1, list2))
        end
        return assert_equal_list
        """
        self.lua_assert_equal_list = self.lua.execute(assert_equal_list)

        assert_equal_list_with_pairs = """
        function pair_exists(list1, key, value)
            i = 1
            for i, item1 in ipairs(list1) do
                if i%2 == 1 then
                    if (list1[i] == key) and (list1[i + 1] == value) then
                        return true
                    end
                end
            end
            return false
        end

        function compare_list_with_pairs(list1, list2)
            if #list1 ~= #list2 or #list1 % 2 == 1 then
                return false
            end
            for i = 1, #list1, 2 do
                if not pair_exists(list2, list1[i], list1[i + 1]) then
                    return false
                end
            end
            return true
        end

        function assert_equal_list_with_pairs(list1, list2)
            assert(compare_list_with_pairs(list1, list2))
        end
        return assert_equal_list_with_pairs
        """
        self.lua_assert_equal_list_with_pairs = self.lua.execute(assert_equal_list_with_pairs)

        compare_val = """
        function compare_val(var1, var2)
            return var1 == var2
        end
        return compare_val
        """
        self.lua_compare_val = self.lua.execute(compare_val)

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

    def test_table_type(self):
        self.redis.lpush(LIST1, VAL2, VAL1)
        script_content = """
        local items = redis.call('LRANGE', KEYS[1], ARGV[1], ARGV[2])
        return type(items)
        """
        script = self.redis.register_script(script_content)
        itemType = script(keys=[LIST1], args=[0, -1])
        self.assertEqual('table', itemType)

    def test_script_hgetall(self):
        myhash = {"k1": "v1"}
        self.redis.hmset("myhash", myhash)
        script_content = """
        return redis.call('HGETALL', KEYS[1])
        """
        script = self.redis.register_script(script_content)
        item = script(keys=["myhash"])
        self.assertIsInstance(item, list)
        self.assertEquals(["k1", "v1"], item)

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

    def test_lua_to_python_none(self):
        lval = self.lua.eval("")
        pval = MockredisScript._lua_to_python(lval)
        self.assertTrue(pval is None)

    def test_lua_to_python_list(self):
        lval = self.lua.eval('{"val1", "val2"}')
        pval = MockredisScript._lua_to_python(lval)
        self.assertIsInstance(pval, list)
        self.assertEqual(["val1", "val2"], pval)

    def test_lua_to_python_long(self):
        lval = self.lua.eval('22')
        pval = MockredisScript._lua_to_python(lval)
        self.assertIsInstance(pval, long)
        self.assertEqual(22, pval)

    def test_lua_to_python_flota(self):
        lval = self.lua.eval('22.2')
        pval = MockredisScript._lua_to_python(lval)
        self.assertIsInstance(pval, float)
        self.assertEqual(22.2, pval)

    def test_lua_to_python_string(self):
        lval = self.lua.eval('"somestring"')
        pval = MockredisScript._lua_to_python(lval)
        self.assertIsInstance(pval, str)
        self.assertEqual("somestring", pval)

    def test_lua_to_python_bool(self):
        lval = self.lua.eval('true')
        pval = MockredisScript._lua_to_python(lval)
        self.assertIsInstance(pval, bool)
        self.assertEqual(True, pval)

    def test_python_to_lua_none(self):
        pval = None
        lval = MockredisScript._python_to_lua(pval)
        is_null = """
        function is_null(var1)
            return var1 == nil
        end
        return is_null
        """
        lua_is_null = self.lua.execute(is_null)
        self.assertTrue(MockredisScript._lua_to_python(lua_is_null(lval)))

    def test_python_to_lua_string(self):
        pval = "somestring"
        lval = MockredisScript._python_to_lua(pval)
        lval_expected = self.lua.eval('"somestring"')
        self.assertEqual("string", self.lua_globals.type(lval))
        self.assertEqual(lval_expected, lval)

    def test_python_to_lua_list(self):
        pval = ["abc", "xyz"]
        lval = MockredisScript._python_to_lua(pval)
        lval_expected = self.lua.eval('{"abc", "xyz"}')
        self.lua_assert_equal_list(lval_expected, lval)

    def test_python_to_lua_dict(self):
        pval = {"k1": "v1", "k2": "v2"}
        lval = MockredisScript._python_to_lua(pval)
        lval_expected = self.lua.eval('{"k1", "v1", "k2", "v2"}')
        self.lua_assert_equal_list_with_pairs(lval_expected, lval)

    def test_python_to_lua_long(self):
        pval = 10L
        lval = MockredisScript._python_to_lua(pval)
        lval_expected = self.lua.eval('10')
        self.assertEqual("number", self.lua_globals.type(lval))
        self.assertTrue(MockredisScript._lua_to_python(self.lua_compare_val(lval_expected, lval)))

    def test_python_to_lua_float(self):
        pval = 10.1
        lval = MockredisScript._python_to_lua(pval)
        lval_expected = self.lua.eval('10.1')
        self.assertEqual("number", self.lua_globals.type(lval))
        self.assertTrue(MockredisScript._lua_to_python(self.lua_compare_val(lval_expected, lval)))

    def test_python_to_lua_boolean(self):
        pval = True
        lval = MockredisScript._python_to_lua(pval)
        self.assertEqual("boolean", self.lua_globals.type(lval))
        self.assertTrue(MockredisScript._lua_to_python(lval))
