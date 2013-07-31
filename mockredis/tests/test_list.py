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
        count = self.redis.lrem(LIST1, 0, VAL1)
        self.assertEqual(2, count)
        self.assertListEqual([VAL2, VAL3, VAL4, VAL2], self.redis.redis[LIST1])

        self.redis.redis[LIST1] = [VAL1, VAL2, VAL1, VAL3, VAL4, VAL2]
        count = self.redis.lrem(LIST1, 1, VAL2)
        self.assertEqual(1, count)
        self.assertListEqual([VAL1, VAL1, VAL3, VAL4, VAL2],
                             self.redis.redis[LIST1])

        self.redis.redis[LIST1] = [VAL1, VAL2, VAL1, VAL3, VAL4, VAL2]
        count = self.redis.lrem(LIST1, 100, VAL1)
        self.assertEqual(2, count)
        self.assertListEqual([VAL2, VAL3, VAL4, VAL2], self.redis.redis[LIST1])

        self.redis.redis[LIST1] = [VAL1, VAL2, VAL1, VAL3, VAL4, VAL2]
        count = self.redis.lrem(LIST1, -1, VAL3)
        self.assertEqual(1, count)
        self.assertListEqual([VAL1, VAL2, VAL1, VAL4, VAL2],
                             self.redis.redis[LIST1])

        self.redis.redis[LIST1] = [VAL1, VAL2, VAL1, VAL3, VAL4, VAL2]
        count = self.redis.lrem(LIST1, -1, VAL2)
        self.assertEqual(1, count)
        self.assertListEqual([VAL1, VAL2, VAL1, VAL3, VAL4],
                             self.redis.redis[LIST1])

        self.redis.redis[LIST1] = [VAL1, VAL2, VAL1, VAL3, VAL4, VAL2]
        count = self.redis.lrem(LIST1, -2, VAL2)
        self.assertEqual(2, count)
        self.assertListEqual([VAL1, VAL1, VAL3, VAL4], self.redis.redis[LIST1])

        count = self.redis.lrem("NON_EXISTENT_LIST", 0, VAL1)
        self.assertEqual(0, count)

    def test_rpoplpush(self):
        self.redis.redis[LIST1] = [VAL1, VAL2]
        self.redis.redis[LIST2] = [VAL3, VAL4]
        transfer_item = self.redis.rpoplpush(LIST1, LIST2)
        self.assertEqual(VAL2, transfer_item)
        self.assertEqual([VAL1], self.redis.redis[LIST1])
        self.assertEqual([VAL2, VAL3, VAL4], self.redis.redis[LIST2])

    def test_lrange_get_all(self):
        """Cases for returning entire list"""
        values = [VAL4, VAL3, VAL2, VAL1]

        self.assertEqual([], self.redis.lrange(LIST1, 0, 6))
        self.assertEqual([], self.redis.lrange(LIST1, 0, -1))
        self.redis.lpush(LIST1, *reversed(values))

        # Check with exact range
        self.assertEqual(values, self.redis.lrange(LIST1, 0, 3))
        # Check with negative index
        self.assertEqual(values, self.redis.lrange(LIST1, 0, -1))
        # Check with range larger than length of list
        self.assertEqual(values, self.redis.lrange(LIST1, 0, 6))

    def test_lrange_get_sublist(self):
        """Cases for returning partial list"""
        values = [VAL4, VAL3, VAL2, VAL1]

        self.assertEqual([], self.redis.lrange(LIST1, 0, 6))
        self.assertEqual([], self.redis.lrange(LIST1, 0, -1))
        self.redis.lpush(LIST1, *reversed(values))

        # Check from left end of the list
        self.assertEqual(values[:2], self.redis.lrange(LIST1, 0, 1))
        # Check from right end of the list
        self.assertEqual(values[2:4], self.redis.lrange(LIST1, 2, 3))
        # Check from right end of the list with negative range
        self.assertEqual(values[-2:], self.redis.lrange(LIST1, -2, -1))
        # Check from middle of the list
        self.assertEqual(values[1:3], self.redis.lrange(LIST1, 1, 2))

    def test_ltrim_retain_all(self):
        values = [VAL4, VAL3, VAL2, VAL1]
        self._reinitialize_list(LIST1, *values)

        self.redis.ltrim(LIST1, 0, -1)
        self.assertEqual(values, self.redis.lrange(LIST1, 0, -1))

        self.redis.ltrim(LIST1, 0, len(values) - 1)
        self.assertEqual(values, self.redis.lrange(LIST1, 0, -1))

        self.redis.ltrim(LIST1, 0, len(values) + 1)
        self.assertEqual(values, self.redis.lrange(LIST1, 0, -1))

        self.redis.ltrim(LIST1, -1 * len(values), -1)
        self.assertEqual(values, self.redis.lrange(LIST1, 0, -1))

        self.redis.ltrim(LIST1, -1 * (len(values) + 1), -1)
        self.assertEqual(values, self.redis.lrange(LIST1, 0, -1))

    def test_ltrim_remove_all(self):
        values = [VAL4, VAL3, VAL2, VAL1]
        self._reinitialize_list(LIST1, *values)

        self.redis.ltrim(LIST1, 2, 1)
        self.assertEqual([], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values)
        self.redis.ltrim(LIST1, -1, -2)
        self.assertEqual([], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values)
        self.redis.ltrim(LIST1, 2, -3)
        self.assertEqual([], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values) 
        self.redis.ltrim(LIST1, -1, 2)
        self.assertEqual([], self.redis.lrange(LIST1, 0, -1))

    def test_ltrim(self):
        values = [VAL4, VAL3, VAL2, VAL1]
        self._reinitialize_list(LIST1, *values)

        self.redis.ltrim(LIST1, 1, 2)
        self.assertEqual(values[1:3], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values)
        self.redis.ltrim(LIST1, -3, -1)
        self.assertEqual(values[-3:], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values)
        self.redis.ltrim(LIST1, 1, 5)
        self.assertEqual(values[1:5], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values)
        self.redis.ltrim(LIST1, -100, 2)
        self.assertEqual(values[-100:3], self.redis.lrange(LIST1, 0, -1))

    def _reinitialize_list(self, key, *values):
        """
        Re-initialize the list
        """
        self.redis.delete(LIST1)
        self.redis.lpush(LIST1, *reversed(values))

