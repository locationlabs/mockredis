from nose.tools import eq_, ok_
from datetime import timedelta

from mockredis.redis import MockRedis


class TestRedisString(object):
    """string tests"""

    def setUp(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_get(self):
        eq_(None, self.redis.get('key'))

        self.redis.redis['key'] = 'value'
        eq_('value', self.redis.get('key'))

    def test_set_no_options(self):
        eq_(None, self.redis.redis.get('key'))

        # test with no options
        self.redis.set('key', 'value')
        eq_('value', self.redis.redis.get('key'))

    def _assert_set_with_options(self, test_cases):
        # local setup
        eq_(None, self.redis.get('key'))

        # checking if the local setup is fine
        self.redis.set('key', 'value')
        eq_('value', self.redis.get('key'))

        category, cases = test_cases
        for case in cases:
            print category
            data, config = case
            print data, config
            # set with creation and expiry options
            key, value, expected_result = data
            result = self.redis.set(key, value, **config)
            eq_(expected_result, result)
            if expected_result is not None:
                eq_(value, self.redis.get(key))
                if 'px' in config and 'ex' in config:
                    px = int(config['px'] / 1000)
                    ex = int(config['ex'])
                    if px > ex:
                        ok_(ex < self.redis.ttl(key) <= px)
                elif 'px' in config:
                    ok_(int(config['px'] / 1000) >= self.redis.ttl(key))
                elif 'ex' in config:
                    ok_(config['ex'] >= self.redis.ttl(key))
            else:
                ok_(value != self.redis.get(key))

    def test_set_with_options(self):
        """test the set function with various combinations of arguments"""

        test_cases = [("1. px and ex are set and nx is always true & set on non-existing key",
                      [(('key1', 'value1', None), dict(ex=20, px=70000, xx=True, nx=True)),
                      (('key2', 'value1', True), dict(ex=20, px=70000, xx=False, nx=True)),
                      (('key3', 'value2', True), dict(ex=20, px=70000, nx=True))]),

                      ("2. px and ex are set and nx is always true & set on existing key",
                      [(('key', 'value1', None), dict(ex=20, px=70000, xx=True, nx=True)),
                      (('key', 'value1', None), dict(ex=20, px=70000, xx=False, nx=True)),
                      (('key', 'value1', None), dict(ex=20, px=70000, nx=True))]),

                      ("3. px and ex are set and xx is always true & set on existing key",
                      [(('key', 'value1', None), dict(ex=20, px=70000, xx=True, nx=True)),
                      (('key', 'value1', True), dict(ex=20, px=70000, xx=True, nx=False)),
                      (('key', 'value4', True), dict(ex=20, px=70000, xx=True))]),

                      ("4. px and ex are set and xx is always true & set on non-existing key",
                      [(('key1', 'value1', None), dict(ex=20, px=70000, xx=True, nx=True)),
                      (('key2', 'value2', None), dict(ex=20, px=70000, xx=True, nx=False)),
                      (('key3', 'value3', None), dict(ex=20, px=70000, xx=True))]),

                      ("5. either nx or xx defined and set to false or none defined" +
                       " & set on existing key",
                      [(('key', 'value1', True), dict(ex=20, px=70000, xx=False)),
                      (('key', 'value2', True), dict(ex=20, px=70000, nx=False)),
                      (('key', 'value3', True), dict(ex=20, px=70000))]),

                      ("6. either nx or xx defined and set to false or none defined" +
                       " & set on non-existing key",
                      [(('key1', 'value1', True), dict(ex=20, px=70000, xx=False)),
                      (('key2', 'value2', True), dict(ex=20, px=70000, nx=False)),
                      (('key3', 'value3', True), dict(ex=20, px=70000))]),

                      ("7: where neither px nor ex defined + set on existing key",
                      [(('key', 'value2', None), dict(xx=True, nx=True)),
                      (('key', 'value2', None), dict(xx=False, nx=True)),
                      (('key', 'value2', True), dict(xx=True, nx=False)),
                      (('key', 'value3', True), dict(xx=True)),
                      (('key', 'value4', None), dict(nx=True)),
                      (('key', 'value4', True), dict(xx=False)),
                      (('key', 'value5', True), dict(nx=False))]),

                      ("8: where neither px nor ex defined + set on non-existing key",
                      [(('key1', 'value1', None), dict(xx=True, nx=True)),
                      (('key2', 'value1', True), dict(xx=False, nx=True)),
                      (('key3', 'value2', None), dict(xx=True, nx=False)),
                      (('key4', 'value3', None), dict(xx=True)),
                      (('key5', 'value4', True), dict(nx=True)),
                      (('key6', 'value4', True), dict(xx=False)),
                      (('key7', 'value5', True), dict(nx=False))]),

                      ("9: where neither nx nor xx defined + set on existing key",
                      [(('key', 'value1', True), dict(ex=20, px=70000)),
                      (('key1', 'value12', True), dict(ex=20)),
                      (('key1', 'value11', True), dict(px=20000))]),

                      ("10: where neither nx nor xx is defined + set on non-existing key",
                      [(('key1', 'value1', True), dict(ex=20, px=70000)),
                      (('key2', 'value2', True), dict(ex=20)),
                      (('key3', 'value3', True), dict(px=20000))])]

        for cases in test_cases:
            yield self._assert_set_with_options, cases

    def _assert_setex(self, seconds):
        eq_(None, self.redis.redis.get('key'))

        ok_(self.redis.setex('key', seconds, 'value'))
        eq_('value', self.redis.redis.get('key'))

        ok_(self.redis.ttl('key'), "expiration was not set correctly")
        if isinstance(seconds, timedelta):
            seconds = seconds.seconds + seconds.days * 24 * 3600
        ok_(0 < self.redis.ttl('key') < seconds)

    def test_setex(self):
        test_cases = [20, timedelta(seconds=20)]
        for case in test_cases:
            yield self._assert_setex, case

    def test_setnx(self):
        ok_(self.redis.setnx('key', 'value'))
        ok_(not self.redis.setnx('key', 'different_value'))
        eq_('value', self.redis.get('key'))
