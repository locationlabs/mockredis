from nose.tools import assert_raises, eq_, ok_

from mockredis.sortedset import SortedSet


class TestSortedSet(object):
    """
    Tests the sorted set data structure, not the redis commands.
    """

    def setup(self):
        self.zset = SortedSet()

    def test_initially_empty(self):
        """
        Sorted set is created empty.
        """
        eq_(0, len(self.zset))

    def test_insert(self):
        """
        Insertion maintains order and uniqueness.
        """
        # insert two values
        ok_(self.zset.insert("one", 1.0))
        ok_(self.zset.insert("two", 2.0))

        # validate insertion
        eq_(2, len(self.zset))
        ok_("one" in self.zset)
        ok_("two" in self.zset)
        ok_(1.0 not in self.zset)
        ok_(2.0 not in self.zset)
        eq_(1.0, self.zset["one"])
        eq_(2.0, self.zset["two"])
        with assert_raises(KeyError):
            self.zset[1.0]
        with assert_raises(KeyError):
            self.zset[2.0]
        eq_(0, self.zset.rank("one"))
        eq_(1, self.zset.rank("two"))
        eq_(None, self.zset.rank(1.0))
        eq_(None, self.zset.rank(2.0))

        # re-insert a value
        ok_(not self.zset.insert("one", 3.0))

        # validate the update
        eq_(2, len(self.zset))
        eq_(3.0, self.zset.score("one"))
        eq_(0, self.zset.rank("two"))
        eq_(1, self.zset.rank("one"))

    def test_remove(self):
        """
        Removal maintains order.
        """
        # insert a few elements
        self.zset["one"] = 1.0
        self.zset["uno"] = 1.0
        self.zset["three"] = 3.0
        self.zset["two"] = 2.0

        # cannot remove a member that is not present
        eq_(False, self.zset.remove("four"))

        # removing an existing entry works
        eq_(True, self.zset.remove("two"))
        eq_(3, len(self.zset))
        eq_(0, self.zset.rank("one"))
        eq_(1, self.zset.rank("uno"))
        eq_(None, self.zset.rank("two"))
        eq_(2, self.zset.rank("three"))

        # delete also works
        del self.zset["uno"]
        eq_(2, len(self.zset))
        eq_(0, self.zset.rank("one"))
        eq_(None, self.zset.rank("uno"))
        eq_(None, self.zset.rank("two"))
        eq_(1, self.zset.rank("three"))

    def test_scoremap(self):
        self.zset["one"] = 1.0
        self.zset["uno"] = 1.0
        self.zset["two"] = 2.0
        self.zset["three"] = 3.0
        eq_([(1.0, "one"), (1.0, "uno")], self.zset.scorerange(1.0, 1.1))
        eq_([(1.0, "one"), (1.0, "uno"), (2.0, "two")],
            self.zset.scorerange(1.0, 2.0))

    def test_scoremap_inclusive(self):
        self.zset["one"] = 1.0
        self.zset["uno"] = 1.0
        self.zset["uno_dot_one"] = 1.1
        self.zset["two"] = 2.0
        self.zset["three"] = 3.0
        eq_([], self.zset.scorerange(1.0, 1.1, start_inclusive=False, end_inclusive=False))
        eq_([(1.0, "one"), (1.0, "uno")], self.zset.scorerange(1.0, 1.1, start_inclusive=True, end_inclusive=False))  # noqa
        eq_([(1.1, "uno_dot_one")], self.zset.scorerange(1.0, 1.1, start_inclusive=False, end_inclusive=True))  # noqa
        eq_([(1.1, "uno_dot_one")], self.zset.scorerange(1.0, 2.0, start_inclusive=False, end_inclusive=False))  # noqa
        eq_([(1.1, "uno_dot_one"), (2.0, "two")],
            self.zset.scorerange(1.0, 3.0, start_inclusive=False, end_inclusive=False))

        eq_([(1.0, "one"), (1.0, "uno"), (1.1, "uno_dot_one")],
            self.zset.scorerange(1.0, 1.1, start_inclusive=True, end_inclusive=True))
        eq_([(1.0, "one"), (1.0, "uno"), (1.1, "uno_dot_one"), (2.0, "two")],
            self.zset.scorerange(1.0, 2.0, start_inclusive=True, end_inclusive=True))
