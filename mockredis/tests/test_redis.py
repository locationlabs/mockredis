from unittest import TestCase
from mockredis import MockRedis

class TestRedis(TestCase):

    def setUp(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_get(self):
        self.assertEqual('', self.redis.get('key'))

        self.redis.redis['key'] = 'value'
        self.assertEqual('value', self.redis.get('key'))

    def test_set(self):
        self.assertEqual(None, self.redis.redis.get('key'))
        
        self.redis.set('key', 'value')
        self.assertEqual('value', self.redis.redis.get('key'))
