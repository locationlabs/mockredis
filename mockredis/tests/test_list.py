from unittest import TestCase
from mockredis.redis import MockRedis


class TestList(TestCase):
    """
    Tests for MockRedis list operations
    """

    def setUp(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_initially_empty(self):
        """
        List is created empty.
        """
        self.assertEqual(0, len(self.redis.redis['test_list']))

    def test_llen(self):
        self.redis.redis['test_list'] = list(['val1', 'val2'])
        self.assertEquals(2, self.redis.llen('test_list'))
        self.redis.redis['test_list'].pop(0)
        self.assertEquals(1, self.redis.llen('test_list'))
        self.redis.redis['test_list'].pop(0)
        self.assertEquals(0, self.redis.llen('test_list'))

    def test_lpop(self):
        self.redis.redis['test_list'] = list(['val1', 'val2'])
        self.assertEquals('val1', self.redis.lpop('test_list'))
        self.assertEquals(1, len(self.redis.redis['test_list']))
        self.assertEquals('val2', self.redis.lpop('test_list'))
        self.assertEquals(0, len(self.redis.redis['test_list']))
        self.assertIsNone(self.redis.lpop('test_list'))

    def test_lpush(self):
        """
        Insertion maintains order but not uniqueness.
        """
        # lpush two values
        self.redis.lpush('test_list', 'val1')
        self.redis.lpush('test_list', 'val2')

        # validate insertion
        self.assertEquals(2, len(self.redis.redis['test_list']))
        self.assertEquals('list', self.redis.type('test_list'))
        self.assertEquals('val2', self.redis.redis['test_list'][0])
        self.assertEquals('val1', self.redis.redis['test_list'][1])

        # insert two more values with one repeated
        self.redis.lpush('test_list', 'val1')
        self.redis.lpush('test_list', 'val3')

        # validate the update
        self.assertEquals(4, len(self.redis.redis['test_list']))
        self.assertEquals('list', self.redis.type('test_list'))
        self.assertEquals('val3', self.redis.redis['test_list'][0])
        self.assertEquals('val1', self.redis.redis['test_list'][1])
        self.assertEquals('val2', self.redis.redis['test_list'][2])
        self.assertEquals('val1', self.redis.redis['test_list'][3])

    def test_rpop(self):
        self.redis.redis['test_list'] = list(['val1', 'val2'])
        self.assertEquals('val2', self.redis.rpop('test_list'))
        self.assertEquals(1, len(self.redis.redis['test_list']))
        self.assertEquals('val1', self.redis.rpop('test_list'))
        self.assertEquals(0, len(self.redis.redis['test_list']))
        self.assertIsNone(self.redis.lpop('test_list'))

    def test_rpush(self):
        """
        Insertion maintains order but not uniqueness.
        """
        # lpush two values
        self.redis.rpush('test_list', 'val1')
        self.redis.rpush('test_list', 'val2')

        # validate insertion
        self.assertEquals(2, len(self.redis.redis['test_list']))
        self.assertEquals('list', self.redis.type('test_list'))
        self.assertEquals('val1', self.redis.redis['test_list'][0])
        self.assertEquals('val2', self.redis.redis['test_list'][1])

        # insert two more values with one repeated
        self.redis.rpush('test_list', 'val1')
        self.redis.rpush('test_list', 'val3')

        # validate the update
        self.assertEquals(4, len(self.redis.redis['test_list']))
        self.assertEquals('list', self.redis.type('test_list'))
        self.assertEquals('val1', self.redis.redis['test_list'][0])
        self.assertEquals('val2', self.redis.redis['test_list'][1])
        self.assertEquals('val1', self.redis.redis['test_list'][2])
        self.assertEquals('val3', self.redis.redis['test_list'][3])
