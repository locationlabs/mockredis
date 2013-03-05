"""
Indexable Skip List recipe from:

   http://code.activestate.com/recipes/576930/
"""
from random import random
from math import log


class Node(object):
    __slots__ = 'value', 'next', 'width'

    def __init__(self, value, next_, width):
        self.value, self.next, self.width = value, next_, width


class End(object):
    'Sentinel object that always compares greater than another object'

    def __cmp__(self, other):
        return 1


# Singleton terminator node
NIL = Node(End(), [], [])


class IndexableSkiplist(object):
    """
    Sorted collection supporting O(lg n) insertion, removal, and lookup by rank.
    """

    def __init__(self, expected_size=100):
        self.size = 0
        self.maxlevels = int(1 + log(expected_size, 2))
        self.head = Node('HEAD', [NIL] * self.maxlevels, [1] * self.maxlevels)

    def __len__(self):
        return self.size

    def __getitem__(self, i):
        """
        Get the value at the given index.
        """
        node = self.head
        i += 1
        for level in reversed(xrange(self.maxlevels)):
            while node.width[level] <= i:
                i -= node.width[level]
                node = node.next[level]
        return node.value

    def rank(self, value):
        """
        Get the index of a value.
        """
        node = self.head
        i = -1
        for level in reversed(xrange(self.maxlevels)):
            while node.next[level].value < value:
                i += node.width[level]
                node = node.next[level]
            if node.next[level].value == value:
                return i + node.width[level]
        return None

    def insert(self, value):
        """
        Insert a value.
        """
        # find first node on each level where node.next[levels].value > value
        chain = [None] * self.maxlevels
        steps_at_level = [0] * self.maxlevels
        node = self.head
        for level in reversed(xrange(self.maxlevels)):
            while node.next[level].value <= value:
                steps_at_level[level] += node.width[level]
                node = node.next[level]
            chain[level] = node

        # insert a link to the newnode at each level
        d = min(self.maxlevels, 1 - int(log(random(), 2.0)))
        newnode = Node(value, [None] * d, [None] * d)
        steps = 0
        for level in xrange(d):
            prevnode = chain[level]
            newnode.next[level] = prevnode.next[level]
            prevnode.next[level] = newnode
            newnode.width[level] = prevnode.width[level] - steps
            prevnode.width[level] = steps + 1
            steps += steps_at_level[level]
        for level in xrange(d, self.maxlevels):
            chain[level].width[level] += 1
        self.size += 1

    def remove(self, value):
        """
        Remove a value.
        """
        # find first node on each level where node.next[levels].value >= value
        chain = [None] * self.maxlevels
        node = self.head
        for level in reversed(xrange(self.maxlevels)):
            while node.next[level].value < value:
                node = node.next[level]
            chain[level] = node
        if value != chain[0].next[0].value:
            raise KeyError('Not Found')

        # remove one link at each level
        d = len(chain[0].next[0].next)
        for level in xrange(d):
            prevnode = chain[level]
            prevnode.width[level] += prevnode.next[level].width[level] - 1
            prevnode.next[level] = prevnode.next[level].next[level]
        for level in xrange(d, self.maxlevels):
            chain[level].width[level] -= 1
        self.size -= 1

    def __iter__(self):
        """
        Iterate over values in sorted order.
        """
        node = self.head.next[0]
        while node is not NIL:
            yield node.value
            node = node.next[0]
