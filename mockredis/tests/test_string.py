from datetime import timedelta

from nose.tools import eq_, ok_

from mockredis.client import get_total_milliseconds
from mockredis.tests.fixtures import raises_response_error, setup


class TestRedisString(object):
    """string tests"""

    def setup(self):
        setup(self)

    def test_get(self):
        eq_(None, self.redis.get('key'))
        self.redis.set('key', 'value')
        eq_('value', self.redis.get('key'))

    def test_mget(self):
        eq_(None, self.redis.get('mget1'))
        eq_(None, self.redis.get('mget2'))
        eq_([None, None], self.redis.mget('mget1', 'mget2'))
        eq_([None, None], self.redis.mget(['mget1', 'mget2']))

        self.redis.set('mget1', 'value1')
        self.redis.set('mget2', 'value2')
        eq_(['value1', 'value2'], self.redis.mget('mget1', 'mget2'))
        eq_(['value1', 'value2'], self.redis.mget(['mget1', 'mget2']))

    def test_set_no_options(self):
        self.redis.set('key', 'value')
        eq_('value', self.redis.get('key'))

    def _assert_set_with_options(self, test_cases):
        """
        Assert conditions for setting a key on the set function.

        The set function can take px, ex, nx and xx kwargs, this function asserts various conditions
        on set depending on the combinations of kwargs: creation mode(nx,xx) and expiration(ex,px).
        E.g. verifying that a non-existent key does not get set if xx=True or gets set with nx=True
        iff it is absent.
        """
        category, existing_key, cases = test_cases
        msg = "Failed in: {}".format(category)
        if existing_key:
            self.redis.set('key', 'value')
        for (key, value, expected_result), config in cases:
            # set with creation mode and expiry options
            result = self.redis.set(key, value, **config)
            eq_(expected_result, result, msg)
            if expected_result is not None:
                # if the set was expected to happen
                self._assert_was_set(key, value, config, msg)
            else:
                # if the set was not expected to happen
                self._assert_not_set(key, value, msg)

    def _assert_not_set(self, key, value, msg):
        """Check that the key and its timeout were not set"""

        # check that the value wasn't updated
        ok_(value != self.redis.get(key), msg)
        if self.redis.exists(key):
            # check that the expiration was not set
            eq_(self.redis.ttl(key), None)
        else:
            # check that the expiration was not set
            eq_(self.redis.ttl(key), -2)

    def _assert_was_set(self, key, value, config, msg, delta=1):
        """Assert that the key was set along with timeout if applicable"""

        eq_(value, self.redis.get(key))
        if "px" not in config and "ex" not in config:
            return
        # px should have been preferred over ex if it was specified
        ttl = self.redis.ttl(key)
        expected_ttl = int(config['px'] / 1000) if "px" in config else config["ex"]
        ok_(expected_ttl - ttl <= delta, msg)

    def test_set_with_options(self):
        """Test the set function with various combinations of arguments"""

        test_cases = [("1. px and ex are set, nx is always true & set on non-existing key",
                      False,
                      [(('key1', 'value1', None), dict(ex=20, px=70000, xx=True, nx=True)),
                      (('key2', 'value1', True), dict(ex=20, px=70000, xx=False, nx=True)),
                      (('key3', 'value2', True), dict(ex=20, px=70000, nx=True))]),

                      ("2. px and ex are set, nx is always true & set on existing key",
                      True,
                      [(('key', 'value1', None), dict(ex=20, px=70000, xx=True, nx=True)),
                      (('key', 'value1', None), dict(ex=20, px=7000, xx=False, nx=True)),
                      (('key', 'value1', None), dict(ex=20, px=70000, nx=True))]),

                      ("3. px and ex are set, xx is always true & set on existing key",
                      True,
                      [(('key', 'value1', None), dict(ex=20, px=70000, xx=True, nx=True)),
                      (('key', 'value1', True), dict(ex=20, px=70000, xx=True, nx=False)),
                      (('key', 'value4', True), dict(ex=20, px=70000, xx=True))]),

                      ("4. px and ex are set, xx is always true & set on non-existing key",
                      False,
                      [(('key1', 'value1', None), dict(ex=20, px=70000, xx=True, nx=True)),
                      (('key2', 'value2', None), dict(ex=20, px=70000, xx=True, nx=False)),
                      (('key3', 'value3', None), dict(ex=20, px=70000, xx=True))]),

                      ("5. either nx or xx defined and set to false or none defined" +
                       " & set on existing key",
                      True,
                      [(('key', 'value1', True), dict(ex=20, px=70000, xx=False)),
                      (('key', 'value2', True), dict(ex=20, px=70000, nx=False)),
                      (('key', 'value3', True), dict(ex=20, px=70000))]),

                      ("6. either nx or xx defined and set to false or none defined" +
                       " & set on non-existing key",
                      False,
                      [(('key1', 'value1', True), dict(ex=20, px=70000, xx=False)),
                      (('key2', 'value2', True), dict(ex=20, px=70000, nx=False)),
                      (('key3', 'value3', True), dict(ex=20, px=70000))]),

                      ("7: where neither px nor ex defined + set on existing key",
                      True,
                      [(('key', 'value2', None), dict(xx=True, nx=True)),
                      (('key', 'value2', None), dict(xx=False, nx=True)),
                      (('key', 'value2', True), dict(xx=True, nx=False)),
                      (('key', 'value3', True), dict(xx=True)),
                      (('key', 'value4', None), dict(nx=True)),
                      (('key', 'value4', True), dict(xx=False)),
                      (('key', 'value5', True), dict(nx=False))]),

                      ("8: where neither px nor ex defined + set on non-existing key",
                      False,
                      [(('key1', 'value1', None), dict(xx=True, nx=True)),
                      (('key2', 'value1', True), dict(xx=False, nx=True)),
                      (('key3', 'value2', None), dict(xx=True, nx=False)),
                      (('key4', 'value3', None), dict(xx=True)),
                      (('key5', 'value4', True), dict(nx=True)),
                      (('key6', 'value4', True), dict(xx=False)),
                      (('key7', 'value5', True), dict(nx=False))]),

                      ("9: where neither nx nor xx defined + set on existing key",
                      True,
                      [(('key', 'value1', True), dict(ex=20, px=70000)),
                      (('key1', 'value12', True), dict(ex=20)),
                      (('key1', 'value11', True), dict(px=20000))]),

                      ("10: where neither nx nor xx is defined + set on non-existing key",
                      False,
                      [(('key1', 'value1', True), dict(ex=20, px=70000)),
                      (('key2', 'value2', True), dict(ex=20)),
                      (('key3', 'value3', True), dict(px=20000))])]

        for cases in test_cases:
            yield self._assert_set_with_options, cases

    def _assert_set_with_timeout(self, seconds):
        """Assert both strict and non-strict that setex sets a key with a value along with a timeout"""

        eq_(None, self.redis_strict.get('key'))
        eq_(None, self.redis.get('key'))

        ok_(self.redis_strict.setex('key', seconds, 'value'))
        ok_(self.redis.setex('key', 'value', seconds))
        eq_('value', self.redis_strict.get('key'))
        eq_('value', self.redis.get('key'))

        ok_(self.redis_strict.ttl('key'), "expiration was not set correctly")
        ok_(self.redis.ttl('key'), "expiration was not set correctly")
        if isinstance(seconds, timedelta):
            seconds = seconds.seconds + seconds.days * 24 * 3600
        ok_(0 < self.redis_strict.ttl('key') <= seconds)
        ok_(0 < self.redis.ttl('key') <= seconds)

    def test_setex(self):
        test_cases = [20, timedelta(seconds=20)]
        for case in test_cases:
            yield self._assert_set_with_timeout, case

    @raises_response_error
    def test_setex_invalid_expiration(self):
        self.redis.setex('key', 'value', -2)

    @raises_response_error
    def test_strict_setex_invalid_expiration(self):
        self.redis_strict.setex('key', -2, 'value')

    @raises_response_error
    def test_setex_zero_expiration(self):
        self.redis.setex('key', 'value', 0)

    @raises_response_error
    def test_strict_setex_zero_expiration(self):
        self.redis_strict.setex('key', 0, 'value')

    def test_mset(self):
        ok_(self.redis.mset({"key1": "hello", "key2": ""}))
        ok_(self.redis.mset(**{"key3": "world", "key2": "there"}))
        eq_(["hello", "there", "world"], self.redis.mget("key1", "key2", "key3"))

    def test_msetnx(self):
        ok_(self.redis.msetnx({"key1": "hello", "key2": "there"}))
        ok_(not self.redis.msetnx(**{"key3": "world", "key2": "there"}))
        eq_(["hello", "there", None], self.redis.mget("key1", "key2", "key3"))

    def test_psetex(self):
        test_cases = [200, timedelta(milliseconds=250)]
        for case in test_cases:
            yield self._assert_set_with_timeout_milliseconds, case

    @raises_response_error
    def test_psetex_invalid_expiration(self):
        self.redis.psetex('key', -20, 'value')

    @raises_response_error
    def test_psetex_zero_expiration(self):
        self.redis.psetex('key', 0, 'value')

    def _assert_set_with_timeout_milliseconds(self, milliseconds):
        """Assert that psetex sets a key with a value along with a timeout"""

        eq_(None, self.redis.get('key'))

        ok_(self.redis.psetex('key', milliseconds, 'value'))
        eq_('value', self.redis.get('key'))

        ok_(self.redis.pttl('key'), "expiration was not set correctly")
        if isinstance(milliseconds, timedelta):
            milliseconds = get_total_milliseconds(milliseconds)

        ok_(0 < self.redis.pttl('key') <= milliseconds)

    def test_setnx(self):
        """Check whether setnx sets a key iff it does not already exist"""

        ok_(self.redis.setnx('key', 'value'))
        ok_(not self.redis.setnx('key', 'different_value'))
        eq_('value', self.redis.get('key'))

    def test_delete(self):
        """Test if delete works"""

        test_cases = [('1', '1'),
                      (('1', '2'), ('1', '2')),
                      (('1', '2', '3'), ('1', '3')),
                      (('1', '2'), '1'),
                      ('1', '2')]
        for case in test_cases:
            yield self._assert_delete, case

    def test_delete_int(self):
        self.redis.set("1", "value")
        eq_(self.redis.delete(1), 1)

    def _assert_delete(self, data):
        """
        Asserts that key(s) deletion along with removing timeouts if any, succeeds as expected
        """
        to_create, to_delete = data
        for key in to_create:
            self.redis.set(key, "value", ex=200)

        eq_(self.redis.delete(*to_delete), len(set(to_create) & set(to_delete)))

        # verify if the keys that were to be deleted, were deleted along with the timeouts.
        for key in set(to_create) & set(to_delete):
            ok_(key not in self.redis)
            eq_(self.redis.ttl(key), -2)

        # verify if the keys not to be deleted, were not deleted and their timeouts not removed.
        for key in set(to_create) - (set(to_create) & set(to_delete)):
            ok_(key in self.redis)
            ok_(self.redis.ttl(key) > 0)

    def test_getset(self):
        eq_(None, self.redis.get('getset_key'))
        eq_(None, self.redis.getset('getset_key', '1'))
        eq_('1', self.redis.getset('getset_key', '2'))
        eq_('2', self.redis.get('getset_key'))
