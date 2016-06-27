from nose.tools import eq_, ok_

from mockredis.tests.fixtures import setup, teardown


class TestRedisConfig(object):
    """Redis config set/get tests"""

    def setup(self):
        setup(self)

    def teardown(self):
        teardown(self)

    def test_config_set(self):
        eq_(self.redis.config_get('config-param'), {})
        self.redis.config_set('config-param', 'value')
        eq_(self.redis.config_get('config-param'), {'config-param': 'value'})
        eq_(self.redis.config_get('config*'), {'config-param': 'value'})

