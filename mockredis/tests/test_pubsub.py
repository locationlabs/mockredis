"""
Tests for pubsub don't yet support verification against redis-server.
"""
from nose.tools import eq_

from mockredis import MockRedis


class TestRedisPubSub(object):

    def setup(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_publish(self):
        channel = 'ch#1'
        msg = 'test message'
        self.redis.publish(channel, msg)
        eq_(self.redis.pubsub[channel], [msg])
