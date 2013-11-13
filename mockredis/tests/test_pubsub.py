from unittest import TestCase

from mockredis import MockRedis


class TestRedisPubSub(TestCase):

    def setUp(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_publish(self):
        channel = 'ch#1'
        msg = 'test message'
        self.redis.publish(channel, msg)
        self.assertListEqual(self.redis.pubsub[channel], [msg])
