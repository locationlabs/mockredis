from unittest import TestCase
from mockredis.redis import MockRedis
from mockredis.tests.test_constants import (
    LIST1, LIST2, VAL1, VAL2, VAL3, VAL4
)


class TestRedisList(TestCase):
    """list tests"""

    def setUp(self):
        self.redis = MockRedis()

    def test_initially_empty(self):
        """
        List is created empty.
        """
        self.assertEqual(0, len(self.redis.redis[LIST1]))

    def test_llen(self):
        self.assertEquals(0, self.redis.llen(LIST1))
        self.redis.redis[LIST1] = [VAL1, VAL2]
        self.assertEquals(2, self.redis.llen(LIST1))
        self.redis.redis[LIST1].pop(0)
        self.assertEquals(1, self.redis.llen(LIST1))
        self.redis.redis[LIST1].pop(0)
        self.assertEquals(0, self.redis.llen(LIST1))

    def test_lpop(self):
        self.redis.redis[LIST1] = [VAL1, VAL2]
        self.assertEquals(VAL1, self.redis.lpop(LIST1))
        self.assertEquals(1, len(self.redis.redis[LIST1]))
        self.assertEquals(VAL2, self.redis.lpop(LIST1))
        self.assertEquals(0, len(self.redis.redis[LIST1]))
        self.assertIsNone(self.redis.lpop(LIST1))

    def test_lpush(self):
        """
        Insertion maintains order but not uniqueness.
        """
        # lpush two values
        self.redis.lpush(LIST1, VAL1)
        self.redis.lpush(LIST1, VAL2)

        # validate insertion
        self.assertEquals("list", self.redis.type(LIST1))
        self.assertEquals([VAL2, VAL1], self.redis.redis[LIST1])

        # insert two more values with one repeated
        self.redis.lpush(LIST1, VAL1, VAL3)

        # validate the update
        self.assertEquals("list", self.redis.type(LIST1))
        self.assertEquals([VAL3, VAL1, VAL2, VAL1], self.redis.redis[LIST1])

    def test_rpop(self):
        self.redis.redis[LIST1] = [VAL1, VAL2]
        self.assertEquals(VAL2, self.redis.rpop(LIST1))
        self.assertEquals(1, len(self.redis.redis[LIST1]))
        self.assertEquals(VAL1, self.redis.rpop(LIST1))
        self.assertEquals(0, len(self.redis.redis[LIST1]))
        self.assertIsNone(self.redis.lpop(LIST1))

    def test_rpush(self):
        """
        Insertion maintains order but not uniqueness.
        """
        # lpush two values
        self.redis.rpush(LIST1, VAL1)
        self.redis.rpush(LIST1, VAL2)

        # validate insertion
        self.assertEquals("list", self.redis.type(LIST1))
        self.assertEquals([VAL1, VAL2], self.redis.redis[LIST1])

        # insert two more values with one repeated
        self.redis.rpush(LIST1, VAL1, VAL3)

        # validate the update
        self.assertEquals("list", self.redis.type(LIST1))
        self.assertEquals([VAL1, VAL2, VAL1, VAL3], self.redis.redis[LIST1])

    def test_lrem(self):
        self.redis.redis[LIST1] = [VAL1, VAL2, VAL1, VAL3, VAL4, VAL2]
        self.redis.lrem(LIST1, 0, VAL1)
        self.assertListEqual([VAL2, VAL3, VAL4, VAL2], self.redis.redis[LIST1])

        self.redis.redis[LIST1] = [VAL1, VAL2, VAL1, VAL3, VAL4, VAL2]
        self.redis.lrem(LIST1, 1, VAL2)
        self.assertListEqual([VAL1, VAL1, VAL3, VAL4, VAL2],
                             self.redis.redis[LIST1])

        self.redis.redis[LIST1] = [VAL1, VAL2, VAL1, VAL3, VAL4, VAL2]
        self.redis.lrem(LIST1, 100, VAL1)
        self.assertListEqual([VAL2, VAL3, VAL4, VAL2], self.redis.redis[LIST1])

        self.redis.redis[LIST1] = [VAL1, VAL2, VAL1, VAL3, VAL4, VAL2]
        self.redis.lrem(LIST1, -1, VAL3)
        self.assertListEqual([VAL1, VAL2, VAL1, VAL4, VAL2],
                             self.redis.redis[LIST1])

        self.redis.redis[LIST1] = [VAL1, VAL2, VAL1, VAL3, VAL4, VAL2]
        self.redis.lrem(LIST1, -1, VAL2)
        self.assertListEqual([VAL1, VAL2, VAL1, VAL3, VAL4],
                             self.redis.redis[LIST1])

        self.redis.redis[LIST1] = [VAL1, VAL2, VAL1, VAL3, VAL4, VAL2]
        self.redis.lrem(LIST1, -2, VAL2)
        self.assertListEqual([VAL1, VAL1, VAL3, VAL4], self.redis.redis[LIST1])

    def test_rpoplpush(self):
        self.redis.redis[LIST1] = [VAL1, VAL2]
        self.redis.redis[LIST2] = [VAL3, VAL4]
        transfer_item = self.redis.rpoplpush(LIST1, LIST2)
        self.assertEqual(VAL2, transfer_item)
        self.assertEqual([VAL1], self.redis.redis[LIST1])
        self.assertEqual([VAL2, VAL3, VAL4], self.redis.redis[LIST2])
