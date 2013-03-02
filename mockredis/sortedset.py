from bisect import bisect_left


class SortedSet(object):
    """
    Redis-style SortedSet implementation.

    Maintains two mappings, one from member to score and one from score to member.

    Insertion and removal are O(N). The bisect operations are O(N log N), but insertion
    and removal for a list are O(N).
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
        return "SortedSet({})".format(self._scores)

    def insert(self, score, member):
        """
        Insert member at score. If member is already present in the
        set, update its score.

        Return whether a member was inserted (True) or updated (False)
        """
        found = self.remove(member)
        index = bisect_left(self._scores, (score, member))
        self._scores.insert(index, (score, member))
        self._members[member] = score
        return found

    def remove(self, member):
        """
        Remove member from the set.

        Return whether a member was removed.
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
        Get the score for a member.
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
