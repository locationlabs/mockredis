from nose.tools import assert_raises, eq_

from mockredis.redis import MockRedis
from mockredis.tests.test_constants import (
    LIST1, LIST2, VAL1, VAL2, VAL3, VAL4
)


class TestRedisList(object):
    """list tests"""

    def setup(self):
        self.redis = MockRedis()

    def test_initially_empty(self):
        """
        List is created empty.
        """
        eq_(0, len(self.redis.lrange(LIST1, 0, -1)))

    def test_llen(self):
        eq_(0, self.redis.llen(LIST1))
        self.redis.lpush(LIST1, VAL1, VAL2)
        eq_(2, self.redis.llen(LIST1))
        self.redis.lpop(LIST1)
        eq_(1, self.redis.llen(LIST1))
        self.redis.lpop(LIST1)
        eq_(0, self.redis.llen(LIST1))

    def test_lpop(self):
        self.redis.rpush(LIST1, VAL1, VAL2)
        eq_(VAL1, self.redis.lpop(LIST1))
        eq_(1, len(self.redis.lrange(LIST1, 0, -1)))
        eq_(VAL2, self.redis.lpop(LIST1))
        eq_(0, len(self.redis.lrange(LIST1, 0, -1)))
        eq_(None, self.redis.lpop(LIST1))
        eq_([], self.redis.keys("*"))

    def test_lpush(self):
        """
        Insertion maintains order but not uniqueness.
        """
        # lpush two values
        self.redis.lpush(LIST1, VAL1)
        self.redis.lpush(LIST1, VAL2)

        # validate insertion
        eq_("list", self.redis.type(LIST1))
        eq_([VAL2, VAL1], self.redis.lrange(LIST1, 0, -1))

        # insert two more values with one repeated
        self.redis.lpush(LIST1, VAL1, VAL3)

        # validate the update
        eq_("list", self.redis.type(LIST1))
        eq_([VAL3, VAL1, VAL2, VAL1], self.redis.lrange(LIST1, 0, -1))

    def test_rpop(self):
        self.redis.rpush(LIST1, VAL1, VAL2)
        eq_(VAL2, self.redis.rpop(LIST1))
        eq_(1, len(self.redis.lrange(LIST1, 0, -1)))
        eq_(VAL1, self.redis.rpop(LIST1))
        eq_(0, len(self.redis.lrange(LIST1, 0, -1)))
        eq_(None, self.redis.lpop(LIST1))
        eq_([], self.redis.keys("*"))

    def test_rpush(self):
        """
        Insertion maintains order but not uniqueness.
        """
        # lpush two values
        self.redis.rpush(LIST1, VAL1)
        self.redis.rpush(LIST1, VAL2)

        # validate insertion
        eq_("list", self.redis.type(LIST1))
        eq_([VAL1, VAL2], self.redis.lrange(LIST1, 0, -1))

        # insert two more values with one repeated
        self.redis.rpush(LIST1, VAL1, VAL3)

        # validate the update
        eq_("list", self.redis.type(LIST1))
        eq_([VAL1, VAL2, VAL1, VAL3], self.redis.lrange(LIST1, 0, -1))

    def test_lrem(self):
        self.redis.rpush(LIST1, VAL1, VAL2, VAL1, VAL3, VAL4, VAL2)
        eq_(2, self.redis.lrem(LIST1, VAL1, 0))
        eq_([VAL2, VAL3, VAL4, VAL2], self.redis.lrange(LIST1, 0, -1))

        del self.redis[LIST1]
        self.redis.rpush(LIST1, VAL1, VAL2, VAL1, VAL3, VAL4, VAL2)
        eq_(1, self.redis.lrem(LIST1, VAL2, 1))
        eq_([VAL1, VAL1, VAL3, VAL4, VAL2], self.redis.lrange(LIST1, 0, -1))

        del self.redis[LIST1]
        self.redis.rpush(LIST1, VAL1, VAL2, VAL1, VAL3, VAL4, VAL2)
        eq_(2, self.redis.lrem(LIST1, VAL1, 100))
        eq_([VAL2, VAL3, VAL4, VAL2], self.redis.lrange(LIST1, 0, -1))

        del self.redis[LIST1]
        self.redis.rpush(LIST1, VAL1, VAL2, VAL1, VAL3, VAL4, VAL2)
        eq_(1, self.redis.lrem(LIST1, VAL3, -1))
        eq_([VAL1, VAL2, VAL1, VAL4, VAL2], self.redis.lrange(LIST1, 0, -1))

        del self.redis[LIST1]
        self.redis.rpush(LIST1, VAL1, VAL2, VAL1, VAL3, VAL4, VAL2)
        eq_(1, self.redis.lrem(LIST1, VAL2, -1))
        eq_([VAL1, VAL2, VAL1, VAL3, VAL4], self.redis.lrange(LIST1, 0, -1))

        del self.redis[LIST1]
        self.redis.rpush(LIST1, VAL1, VAL2, VAL1, VAL3, VAL4, VAL2)
        eq_(2, self.redis.lrem(LIST1, VAL2, -2))
        eq_([VAL1, VAL1, VAL3, VAL4], self.redis.lrange(LIST1, 0, -1))

        del self.redis[LIST1]
        self.redis.rpush(LIST1, VAL1)
        eq_(1, self.redis.lrem(LIST1, VAL1))
        eq_([], self.redis.lrange(LIST1, 0, -1))
        eq_([], self.redis.keys("*"))

        eq_(0, self.redis.lrem("NON_EXISTENT_LIST", VAL1, 0))

    def test_rpoplpush(self):
        self.redis.rpush(LIST1, VAL1, VAL2)
        self.redis.rpush(LIST2, VAL3, VAL4)
        transfer_item = self.redis.rpoplpush(LIST1, LIST2)
        eq_(VAL2, transfer_item)
        eq_([VAL1], self.redis.lrange(LIST1, 0, -1))
        eq_([VAL2, VAL3, VAL4], self.redis.lrange(LIST2, 0, -1))

    def test_rpoplpush_with_empty_source(self):
        # source list is empty
        self.redis.redis[LIST1] = []
        self.redis.rpush(LIST2, VAL3, VAL4)
        transfer_item = self.redis.rpoplpush(LIST1, LIST2)
        eq_(None, transfer_item)
        eq_([], self.redis.lrange(LIST1, 0, -1))
        # nothing has been added to the destination queue
        eq_([VAL3, VAL4], self.redis.lrange(LIST2, 0, -1))

    def test_rpoplpush_source_with_empty_string(self):
        # source list contains empty string
        self.redis.redis[LIST1] = ['']
        self.redis.rpush(LIST2, VAL3, VAL4)
        eq_(1, self.redis.llen(LIST1))
        eq_(2, self.redis.llen(LIST2))

        transfer_item = self.redis.rpoplpush(LIST1, LIST2)
        eq_('', transfer_item)
        eq_(0, self.redis.llen(LIST1))
        eq_(3, self.redis.llen(LIST2))
        eq_([], self.redis.lrange(LIST1, 0, -1))
        # empty string is added to the destination queue
        eq_(['', VAL3, VAL4], self.redis.lrange(LIST2, 0, -1))

    def test_lrange_get_all(self):
        """Cases for returning entire list"""
        values = [VAL4, VAL3, VAL2, VAL1]

        eq_([], self.redis.lrange(LIST1, 0, 6))
        eq_([], self.redis.lrange(LIST1, 0, -1))
        self.redis.lpush(LIST1, *reversed(values))

        # Check with exact range
        eq_(values, self.redis.lrange(LIST1, 0, 3))
        # Check with negative index
        eq_(values, self.redis.lrange(LIST1, 0, -1))
        # Check with range larger than length of list
        eq_(values, self.redis.lrange(LIST1, 0, 6))

    def test_lrange_get_sublist(self):
        """Cases for returning partial list"""
        values = [VAL4, VAL3, VAL2, VAL1]

        eq_([], self.redis.lrange(LIST1, 0, 6))
        eq_([], self.redis.lrange(LIST1, 0, -1))
        self.redis.lpush(LIST1, *reversed(values))

        # Check from left end of the list
        eq_(values[:2], self.redis.lrange(LIST1, 0, 1))
        # Check from right end of the list
        eq_(values[2:4], self.redis.lrange(LIST1, 2, 3))
        # Check from right end of the list with negative range
        eq_(values[-2:], self.redis.lrange(LIST1, -2, -1))
        # Check from middle of the list
        eq_(values[1:3], self.redis.lrange(LIST1, 1, 2))

    def test_ltrim_retain_all(self):
        values = [VAL4, VAL3, VAL2, VAL1]
        self._reinitialize_list(LIST1, *values)

        self.redis.ltrim(LIST1, 0, -1)
        eq_(values, self.redis.lrange(LIST1, 0, -1))

        self.redis.ltrim(LIST1, 0, len(values) - 1)
        eq_(values, self.redis.lrange(LIST1, 0, -1))

        self.redis.ltrim(LIST1, 0, len(values) + 1)
        eq_(values, self.redis.lrange(LIST1, 0, -1))

        self.redis.ltrim(LIST1, -1 * len(values), -1)
        eq_(values, self.redis.lrange(LIST1, 0, -1))

        self.redis.ltrim(LIST1, -1 * (len(values) + 1), -1)
        eq_(values, self.redis.lrange(LIST1, 0, -1))

    def test_ltrim_remove_all(self):
        values = [VAL4, VAL3, VAL2, VAL1]
        self._reinitialize_list(LIST1, *values)

        self.redis.ltrim(LIST1, 2, 1)
        eq_([], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values)
        self.redis.ltrim(LIST1, -1, -2)
        eq_([], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values)
        self.redis.ltrim(LIST1, 2, -3)
        eq_([], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values)
        self.redis.ltrim(LIST1, -1, 2)
        eq_([], self.redis.lrange(LIST1, 0, -1))

    def test_ltrim(self):
        values = [VAL4, VAL3, VAL2, VAL1]
        self._reinitialize_list(LIST1, *values)

        self.redis.ltrim(LIST1, 1, 2)
        eq_(values[1:3], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values)
        self.redis.ltrim(LIST1, -3, -1)
        eq_(values[-3:], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values)
        self.redis.ltrim(LIST1, 1, 5)
        eq_(values[1:5], self.redis.lrange(LIST1, 0, -1))

        self._reinitialize_list(LIST1, *values)
        self.redis.ltrim(LIST1, -100, 2)
        eq_(values[-100:3], self.redis.lrange(LIST1, 0, -1))

    def test_lset(self):
        with assert_raises(Exception):
            self.redis.lset(LIST1, 1, VAL1)

        self.redis.lpush(LIST1, VAL2)
        eq_([VAL2], self.redis.lrange(LIST1, 0, -1))

        with assert_raises(Exception):
            self.redis.lset(LIST1, 1, VAL1)

        self.redis.lset(LIST1, 0, VAL1)
        eq_([VAL1], self.redis.lrange(LIST1, 0, -1))

    def test_push_pop_returns_str(self):
        key = 'l'
        values = ['5', 5, [], {}]
        for v in values:
            self.redis.rpush(key, v)
            eq_(self.redis.lpop(key),
                str(v))

    def _reinitialize_list(self, key, *values):
        """
        Re-initialize the list
        """
        self.redis.delete(LIST1)
        self.redis.lpush(LIST1, *reversed(values))
