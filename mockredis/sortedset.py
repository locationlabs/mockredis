from bisect import bisect_left, bisect_right


class SortedSet(object):
    """
    Redis-style SortedSet implementation.

    Maintains two internal data structures:

    1. A multimap from score to member implemented using a sorted list of (score, member) pairs.
    2. A dictionary from member to score.

    Insertion and removal are O(N). The bisect operations used to maintain the multimap are
    O(N log N), but insertion into and removal from a list are O(N). A balanced tree implementation
    (see: bintrees) would be more efficient, but doesn't provide multimapping and the simple
    workaround of mapping keys to other collections makes the rank() operation more complex.
    """
    def __init__(self):
        """
        Create an empty sorted set.
        """
        # sorted list of score to member
        self._scores = []
        # dictionary from member to score
        self._members = {}

    def clear(self):
        """
        Remove all members and scores from the sorted set.
        """
        self.__init__()

    def __len__(self):
        return len(self._members)

    def __contains__(self, member):
        return member in self._members

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "SortedSet({})".format(self._scores)

    def __setitem__(self, member, score):
        """
        Insert member with score. If member is already present in the
        set, update its score.
        """
        self.insert(member, score)

    def __delitem__(self, member):
        """
        Remove member from the set.
        """
        self.remove(member)

    def __getitem__(self, member):
        """
        Get the score for a member.
        """
        if type(member) is slice:
            raise TypeError("Slicing not supported")
        return self._members[member]

    def __iter__(self):
        return self._scores.__iter__()

    def __reversed__(self):
        return self._scores.__reversed__()

    def insert(self, member, score):
        """
        Identical to __setitem, but returns whether a member was
        inserted (True) or updated (False)
        """
        found = self.remove(member)
        index = bisect_left(self._scores, (score, member))
        self._scores.insert(index, (score, member))
        self._members[member] = score
        return not found

    def remove(self, member):
        """
        Identical to __delitem__, but returns whether a member was removed.
        """
        if member not in self:
            return False
        score = self._members[member]
        score_index = bisect_left(self._scores, (score, member))
        del self._scores[score_index]
        del self._members[member]
        return True

    def score(self, member):
        """
        Identical to __getitem__, but returns None instead of raising
        KeyError if member is not found.
        """
        return self._members.get(member)

    def rank(self, member):
        """
        Get the rank (index of a member).
        """
        score = self._members.get(member)
        if score is None:
            return None
        return bisect_left(self._scores, (score, member))

    def range(self, start, end, desc=False):
        """
        Return (score, member) pairs between min and max ranks.
        """
        if not self:
            return []

        end += 1

        if desc:
            return [self._scores[len(self) - rank - 1] for rank in xrange(start, end)]
        else:
            return self._scores[start:end]

    def scorerange(self, start, end):
        """
        Return (score, member) pairs between min and max scores.
        """
        if not self:
            return []

        left = bisect_left(self._scores, (start,))
        right = bisect_right(self._scores, (end,))
        while right < len(self) and self._scores[right][0] == end:
            right += 1

        return self._scores[left:right]

    def min_score(self):
        return self._scores[0][0]

    def max_score(self):
        return self._scores[-1][0]
