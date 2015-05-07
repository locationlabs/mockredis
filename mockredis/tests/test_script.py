"""
Tests for scripts don't yet support verification against redis-server.
"""
from hashlib import sha1
from unittest.case import SkipTest
import sys
import threading

from nose.tools import assert_raises, eq_, ok_

from mockredis import MockRedis
from mockredis.exceptions import RedisError
from mockredis.script import Script as MockRedisScript
from mockredis.tests.test_constants import (
    LIST1, LIST2,
    SET1,
    VAL1, VAL2, VAL3, VAL4,
    LPOP_SCRIPT
)
from mockredis.tests.fixtures import raises_response_error


if sys.version_info >= (3, 0):
    long = int


class TestScript(object):
    """
    Tests for MockRedis scripting operations
    """

    def setup(self):
        self.redis = MockRedis(load_lua_dependencies=False)
        self.LPOP_SCRIPT_SHA = sha1(LPOP_SCRIPT.encode("utf-8")).hexdigest()

        try:
            lua, lua_globals = MockRedisScript._import_lua(load_dependencies=False)
        except RuntimeError:
            raise SkipTest("mockredispy was not installed with lua support")

        self.lua = lua
        self.lua_globals = lua_globals

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
        eq_([VAL2, VAL1], self.redis.lrange(LIST1, 0, -1))

    def test_register_script_lpop(self):
        self.redis.lpush(LIST1, VAL2, VAL1)

        # lpop one value
        script_content = "return redis.call('LPOP', KEYS[1])"
        script = self.redis.register_script(script_content)
        list_item = script(keys=[LIST1])

        # validate lpop
        eq_(VAL1, list_item)
        eq_([VAL2], self.redis.lrange(LIST1, 0, -1))

    def test_register_script_rpoplpush(self):
        self.redis.lpush(LIST1, VAL2, VAL1)
        self.redis.lpush(LIST2, VAL4, VAL3)

        # rpoplpush
        script_content = "redis.call('RPOPLPUSH', KEYS[1], KEYS[2])"
        script = self.redis.register_script(script_content)
        script(keys=[LIST1, LIST2])

        # validate rpoplpush
        eq_([VAL1], self.redis.lrange(LIST1, 0, -1))
        eq_([VAL2, VAL3, VAL4], self.redis.lrange(LIST2, 0, -1))

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

        # validate rpop and then lpush
        eq_([VAL1], self.redis.lrange(LIST1, 0, -1))
        eq_([VAL2, VAL3, VAL4], self.redis.lrange(LIST2, 0, -1))

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
        eq_(VAL3, list_item)
        eq_([VAL4], redis2.lrange(LIST1, 0, -1))
        eq_([VAL1, VAL2], self.redis.lrange(LIST1, 0, -1))

    def test_eval_lpush(self):
        # lpush two values
        script_content = "redis.call('LPUSH', KEYS[1], ARGV[1], ARGV[2])"
        self.redis.eval(script_content, 1, LIST1, VAL1, VAL2)

        # validate insertion
        eq_([VAL2, VAL1], self.redis.lrange(LIST1, 0, -1))

    def test_eval_lpop(self):
        self.redis.lpush(LIST1, VAL2, VAL1)

        # lpop one value
        script_content = "return redis.call('LPOP', KEYS[1])"
        list_item = self.redis.eval(script_content, 1, LIST1)

        # validate lpop
        eq_(VAL1, list_item)
        eq_([VAL2], self.redis.lrange(LIST1, 0, -1))

    def test_eval_lrem(self):
        self.redis.delete(LIST1)
        self.redis.lpush(LIST1, VAL1)

        # lrem one value
        script_content = "return redis.call('LREM', KEYS[1], 0, ARGV[1])"
        value = self.redis.eval(script_content, 1, LIST1, VAL1)

        eq_(value, 1)

    def test_eval_zadd(self):
        # The score and member are reversed when the client is not strict.
        self.redis.strict = False
        script_content = "return redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])"
        self.redis.eval(script_content, 1, SET1, 42, VAL1)

        eq_(42, self.redis.zscore(SET1, VAL1))

    def test_eval_zrangebyscore(self):
        # Make sure the limit is removed.
        script = "return redis.call('zrangebyscore',KEYS[1],ARGV[1],ARGV[2])"
        self.eval_zrangebyscore(script)

    def test_eval_zrangebyscore_with_limit(self):
        # Make sure the limit is removed.
        script = ("return redis.call('zrangebyscore', "
                  "KEYS[1], ARGV[1], ARGV[2], 'LIMIT', 0, 2)")

        self.eval_zrangebyscore(script)

    def eval_zrangebyscore(self, script):
        self.redis.strict = False
        self.redis.zadd(SET1, VAL1, 1)
        self.redis.zadd(SET1, VAL2, 2)

        eq_([],           self.redis.eval(script, 1, SET1, 0, 0))
        eq_([VAL1],       self.redis.eval(script, 1, SET1, 0, 1))
        eq_([VAL1, VAL2], self.redis.eval(script, 1, SET1, 0, 2))
        eq_([VAL2],       self.redis.eval(script, 1, SET1, 2, 2))

    def test_table_type(self):
        self.redis.lpush(LIST1, VAL2, VAL1)
        script_content = """
        local items = redis.call('LRANGE', KEYS[1], ARGV[1], ARGV[2])
        return type(items)
        """
        script = self.redis.register_script(script_content)
        itemType = script(keys=[LIST1], args=[0, -1])
        eq_('table', itemType)

    def test_script_hgetall(self):
        myhash = {"k1": "v1"}
        self.redis.hmset("myhash", myhash)
        script_content = """
        return redis.call('HGETALL', KEYS[1])
        """
        script = self.redis.register_script(script_content)
        item = script(keys=["myhash"])
        ok_(isinstance(item, list))
        eq_(["k1", "v1"], item)

    def test_evalsha(self):
        self.redis.lpush(LIST1, VAL1)
        script = LPOP_SCRIPT
        sha = self.LPOP_SCRIPT_SHA

        # validator error when script not registered
        with assert_raises(RedisError) as redis_error:
            self.redis.evalsha(self.LPOP_SCRIPT_SHA, 1, LIST1)

        eq_("Sha not registered", str(redis_error.exception))

        with assert_raises(RedisError):
            self.redis.evalsha(self.LPOP_SCRIPT_SHA, 1, LIST1)

        # load script and then evalsha
        eq_(sha, self.redis.script_load(script))
        eq_(VAL1, self.redis.evalsha(sha, 1, LIST1))
        eq_(0, self.redis.llen(LIST1))

    def test_script_exists(self):
        script = LPOP_SCRIPT
        sha = self.LPOP_SCRIPT_SHA
        eq_([False], self.redis.script_exists(sha))
        self.redis.register_script(script)
        eq_([True], self.redis.script_exists(sha))

    def test_script_flush(self):
        script = LPOP_SCRIPT
        sha = self.LPOP_SCRIPT_SHA
        self.redis.register_script(script)
        eq_([True], self.redis.script_exists(sha))
        self.redis.script_flush()
        eq_([False], self.redis.script_exists(sha))

    def test_script_load(self):
        script = LPOP_SCRIPT
        sha = self.LPOP_SCRIPT_SHA
        eq_([False], self.redis.script_exists(sha))
        eq_(sha, self.redis.script_load(script))
        eq_([True], self.redis.script_exists(sha))

    def test_lua_to_python_none(self):
        lval = self.lua.eval("")
        pval = MockRedisScript._lua_to_python(lval)
        ok_(pval is None)

    def test_lua_to_python_list(self):
        lval = self.lua.eval('{"val1", "val2"}')
        pval = MockRedisScript._lua_to_python(lval)
        ok_(isinstance(pval, list))
        eq_(["val1", "val2"], pval)

    def test_lua_to_python_long(self):
        lval = self.lua.eval('22')
        pval = MockRedisScript._lua_to_python(lval)
        ok_(isinstance(pval, long))
        eq_(22, pval)

    def test_lua_to_python_flota(self):
        lval = self.lua.eval('22.2')
        pval = MockRedisScript._lua_to_python(lval)
        ok_(isinstance(pval, float))
        eq_(22.2, pval)

    def test_lua_to_python_string(self):
        lval = self.lua.eval('"somestring"')
        pval = MockRedisScript._lua_to_python(lval)
        ok_(isinstance(pval, str))
        eq_("somestring", pval)

    def test_lua_to_python_bool(self):
        lval = self.lua.eval('true')
        pval = MockRedisScript._lua_to_python(lval)
        ok_(isinstance(pval, bool))
        eq_(True, pval)

    def test_python_to_lua_none(self):
        pval = None
        lval = MockRedisScript._python_to_lua(pval)
        is_null = """
        function is_null(var1)
            return var1 == nil
        end
        return is_null
        """
        lua_is_null = self.lua.execute(is_null)
        ok_(MockRedisScript._lua_to_python(lua_is_null(lval)))

    def test_python_to_lua_string(self):
        pval = "somestring"
        lval = MockRedisScript._python_to_lua(pval)
        lval_expected = self.lua.eval('"somestring"')
        eq_("string", self.lua_globals.type(lval))
        eq_(lval_expected, lval)

    def test_python_to_lua_list(self):
        pval = ["abc", "xyz"]
        lval = MockRedisScript._python_to_lua(pval)
        lval_expected = self.lua.eval('{"abc", "xyz"}')
        self.lua_assert_equal_list(lval_expected, lval)

    def test_python_to_lua_dict(self):
        pval = {"k1": "v1", "k2": "v2"}
        lval = MockRedisScript._python_to_lua(pval)
        lval_expected = self.lua.eval('{"k1", "v1", "k2", "v2"}')
        self.lua_assert_equal_list_with_pairs(lval_expected, lval)

    def test_python_to_lua_long(self):
        pval = long(10)
        lval = MockRedisScript._python_to_lua(pval)
        lval_expected = self.lua.eval('10')
        eq_("number", self.lua_globals.type(lval))
        ok_(MockRedisScript._lua_to_python(self.lua_compare_val(lval_expected, lval)))

    def test_python_to_lua_float(self):
        pval = 10.1
        lval = MockRedisScript._python_to_lua(pval)
        lval_expected = self.lua.eval('10.1')
        eq_("number", self.lua_globals.type(lval))
        ok_(MockRedisScript._lua_to_python(self.lua_compare_val(lval_expected, lval)))

    def test_python_to_lua_boolean(self):
        pval = True
        lval = MockRedisScript._python_to_lua(pval)
        eq_("boolean", self.lua_globals.type(lval))
        ok_(MockRedisScript._lua_to_python(lval))

    def test_lua_ok_return(self):
        script_content = "return {ok='OK'}"
        script = self.redis.register_script(script_content)
        eq_('OK', script())

    @raises_response_error
    def test_lua_err_return(self):
        script_content = "return {err='ERROR Some message'}"
        script = self.redis.register_script(script_content)
        script()

    def test_concurrent_lua(self):
        script_content = """
local entry = redis.call('HGETALL', ARGV[1])
redis.call('HSET', ARGV[1], 'kk', 'vv')
return entry
"""
        script = self.redis.register_script(script_content)

        for i in range(500):
            self.redis.hmset(i, {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})

        def lua_thread():
            for i in range(500):
                script(args=[i])

        active_threads = []
        for i in range(10):
            thread = threading.Thread(target=lua_thread)
            active_threads.append(thread)
            thread.start()

        for thread in active_threads:
            thread.join()
