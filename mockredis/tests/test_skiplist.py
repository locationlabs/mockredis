from unittest import TestCase
from mockredis.skiplist import IndexableSkiplist
from random import shuffle


class TestIndexableSkiplist(TestCase):

    def setUp(self):
        self.slist = IndexableSkiplist()

    def test_insert(self):
        # create a list of values in random order
        values = range(10, 20)
        shuffle(values)

        # insert items (in random order)
        for index, value in enumerate(values):
            self.slist.insert(value)
            # verify membership XXX
            self.assertTrue(value in self.slist)
            # verify length
            self.assertEquals(1 + index, len(self.slist))

        # verify iteration is in sort order
        self.assertEquals(range(10, 20), [x for x in self.slist])

        # verify index lookup - note that index != value
        for index, value in enumerate(range(10, 20)):
            self.assertEquals(value, self.slist[index])
            self.assertEquals(index, self.slist.rank(value))

    def test_remove(self):

        # create a list of values in random order
        values = range(10, 20)
        shuffle(values)

        # insert items
        for index, value in enumerate(values):
            self.slist.insert(value)

        shuffle(values)
        # remove items (in random order)
        for index, value in enumerate(values):
            self.slist.remove(value)
            # verify non-membership
            self.assertFalse(value in self.slist)
            # verify length
            self.assertEquals(10 - index - 1, len(self.slist))
