from collections import defaultdict
from datetime import datetime, timedelta
from operator import add
from random import randint
from .lock import MockRedisLock
from .sortedset import SortedSet


def _get_total_seconds(td):
    """
    for python 2.6 support
    """
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6


class MockRedis(object):
    """
    A Mock for a redis-py Redis object

    Expire functionality must be explicitly
    invoked using do_expire(time). Automatic
    expiry is NOT supported.
    """

    # The 'Redis' store
    redis = defaultdict(dict)
    timeouts = defaultdict(dict)
    # The pipeline
    pipe = None

    def __init__(self, strict=False):
        """
        Initialize as either StrictRedis or Redis.

        Defaults to non-strict.
        """
        self.strict = strict

    def type(self, key):
        if key not in self.redis:
            return 'none'
        _type = type(self.redis[key])
        if _type is dict:
            return 'hash'
        elif _type is str:
            return 'string'
        elif _type is set:
            return 'set'
        elif _type is list:
            return 'list'
        elif _type is SortedSet:
            return 'zset'
        return 'none'

    def echo(self, msg):
        return msg

    def get(self, key):

        # Override the default dict
        result = None if key not in self.redis else self.redis[key]
        return result

    def set(self, key, value):

        self.redis[key] = str(value)
        return True

    def keys(self, pattern):
        """Emulate keys."""
        import re

        # Make a regex out of pattern. The only special matching character we look for is '*'
        regex = '^' + pattern.replace('*', '.*') + '$'

        # Find every key that matches the pattern
        result = [key for key in self.redis.keys() if re.match(regex, key)]

        return result

    def lock(self, key, timeout=0, sleep=0):
        """Emulate lock."""

        return MockRedisLock(self, key, timeout, sleep)

    def pipeline(self):
        """Emulate a redis-python pipeline."""
        # Prevent a circular import
        from pipeline import MockRedisPipeline

        if self.pipe is None:
            self.pipe = MockRedisPipeline(self.redis, self.timeouts)
        return self.pipe

    def delete(self, key):
        """Emulate delete."""

        if key in self.redis:
            del self.redis[key]

    def exists(self, key):
        """Emulate exists."""

        return key in self.redis

    def decr(self, key, decrement=1):
        """Emulate decr."""

        previous_value = long(self.redis.get(key, '0'))
        self.redis[key] = str(previous_value - decrement)
        return long(self.redis[key])

    def incr(self, key, increment=1):

        previous_value = long(self.redis.get(key, '0'))
        self.redis[key] = str(previous_value + increment)
        return long(self.redis[key])

    def execute(self):
        """Emulate the execute method. All piped commands are executed immediately
        in this mock, so this is a no-op."""

        pass

    def hexists(self, hashkey, attribute):
        """Emulate hexists."""

        return attribute in self.redis[hashkey]

    def hget(self, hashkey, attribute):
        """Emulate hget."""

        # Return '' if the attribute does not exist
        result = self.redis[hashkey][attribute] if attribute in self.redis[hashkey] \
            else None
        return result

    def hgetall(self, hashkey):
        """Emulate hgetall."""

        return self.redis[hashkey]

    def hdel(self, hashkey, *keys):
        """Emulate hdel"""

        deleted = map(self.redis[hashkey].pop, keys)
        return len(deleted)

    def hlen(self, hashkey):
        """Emulate hlen."""

        return len(self.redis[hashkey])

    def hmset(self, hashkey, value):
        """Emulate hmset."""

        # Iterate over every key:value in the value argument.
        for attributekey, attributevalue in value.items():
            self.redis[hashkey][attributekey] = attributevalue

    def hset(self, key, attribute, value):
        """Emulate hset."""

        if key not in self.redis:
            self.redis['key'] = {}
        else:
            if type(self.redis[key]) != dict:
                raise ValueError("Type mismatch for key={key}".format(key=key))

        self.redis[key][attribute] = str(value)

    def hincrby(self, key, attribute, increment=1):

        # inititalize hset and value if required
        if key not in self.redis:
            self.redis['key'] = {}
        previous_value = long(self.redis[key].get(attribute, '0'))

        self.redis[key][attribute] = str(previous_value + increment)
        return long(self.redis[key][attribute])

    def expire(self, key, seconds, currenttime=datetime.now()):
        """Emulate expire"""

        if key in self.redis:
            self.timeouts[key] = currenttime + timedelta(seconds=seconds)
            return 1
        return 0

    def ttl(self, key, currenttime=datetime.now()):
        """
        Emulate ttl
        do_expire to get valid values
        """

        self.do_expire(currenttime)
        return -1 if key not in self.timeouts else _get_total_seconds(self.timeouts[key] - currenttime)

    def do_expire(self, currenttime=datetime.now()):
        """
        Expire objects assuming now == time

        """

        for key, value in self.timeouts.items():
            if value - currenttime < timedelta(0):
                del self.timeouts[key]

    def watch(self, *argv, **kwargs):
        """
        Mock does not support command buffering so watch
        is a no-op
        """
        pass

    def multi(self, *argv, **kwargs):
        """
        Mock does not support command buffering so multi
        is a no-op
        """
        pass

    def lrange(self, key, start, stop):
        """Emulate lrange."""

        # Does the set at this key already exist?
        if key in self.redis:
            # Yes, add this to the list
            return map(str, self.redis[key][start:stop + 1 if stop != -1 else None])
        else:
            # No, override the defaultdict's default and create the list
            self.redis[key] = list([])

    def lindex(self, key, index):
        """Emulate lindex."""

        if not key in self.redis:
            self.redis[key] = list([])
        try:
            return self.redis[key][index]
        except (IndexError):
            # Redis returns nil if the index doesn't exist
            pass

    def lpop(self, key):
        """Emulate lpop."""

        if key in self.redis:
            try:
                return str(self.redis[key].pop(0))
            except (IndexError):
                # Redis returns nil if popping from an empty list
                pass

    def rpush(self, key, *args):
        """Emulate rpush."""

        # Does the set at this key already exist?
        if not key in self.redis:
            self.redis[key] = list([])
        for arg in args:
            self.redis[key].append(arg)

    def sadd(self, key, *values):
        """Emulate sadd."""

        # Does the set at this key already exist?
        if key in self.redis:
            # Yes, add this to the set converting values
            # to string
            self.redis[key].update(map(str, values))
        else:
            # No, override the defaultdict's default and create the set
            self.redis[key] = set(map(str, values))

    def srem(self, key, member):
        """Emulate a srem."""

        self.redis[key].discard(member)
        return self

    def srandmember(self, key):
        """Emulate a srandmember."""
        length = len(self.redis[key])
        rand_index = randint(0, length - 1)

        i = 0
        for set_item in self.redis[key]:
            if i == rand_index:
                return set_item

    def smembers(self, key):
        """Emulate smembers."""

        if key not in self.redis:
            return set([])
        else:
            return self.redis[key]

    def flushdb(self):
        self.redis.clear()
        self.timeouts.clear()

    #### SORTED SET COMMANDS ####
    def zadd(self, name, *args, **kwargs):
        zset = self._get_zset(name, "ZADD", create=True)

        pieces = []

        # args
        if len(args) % 2 != 0:
            raise ValueError("ZADD requires an equal number of "
                             "values and scores")
        for i in xrange(len(args) / 2):
            # interpretation of args order depends on whether Redis
            # or StrictRedis is used
            score = args[2 * i + (0 if self.strict else 1)]
            member = args[2 * i + (1 if self.strict else 0)]
            pieces.append((member, score))

        # kwargs
        pieces.extend(kwargs.items())

        insert_count = lambda member, score: 1 if zset.insert(member, float(score)) else 0
        return sum((insert_count(member, score) for member, score in pieces))

    def zcard(self, name):
        zset = self._get_zset(name, "ZCARD")

        return len(zset) if zset is not None else 0

    def zcount(self, name, min_, max_):
        zset = self._get_zset(name, "ZCOUNT")

        if not zset:
            return 0

        return len(zset.scorerange(float(min_), float(max_)))

    def zincrby(self, name, value, amount=1):
        zset = self._get_zset(name, "ZINCRBY", create=True)

        score = zset.score(value) or 0.0
        score += float(amount)
        zset[value] = score
        return score

    def zinterstore(self, dest, keys, aggregate=None):
        aggregate_func = self._aggregate_func(aggregate)

        members = {}

        for key in keys:
            zset = self._get_zset(key, "ZINTERSTORE")
            if not zset:
                return 0

            for score, member in zset:
                members.setdefault(member, []).append(score)

        intersection = SortedSet()
        for member, scores in members.iteritems():
            if len(scores) != len(keys):
                continue
            intersection[member] = reduce(aggregate_func, scores)

        # always override existing keys
        self.redis[dest] = intersection
        return len(intersection)

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float):
        zset = self._get_zset(name, "ZRANGE", create=True)

        start, end = self._translate_range(len(zset), start, end)
        if start == len(zset) or end < start:
            return []

        func = self._range_func(withscores, score_cast_func)
        return [func(item) for item in zset.range(start, end, desc)]

    def zrangebyscore(self, name, min_, max_, start=None, num=None,
                      withscores=False, score_cast_func=float):
        if (start is None and num is not None) or (start is not None and num is None):
            raise TypeError('`start` and `num` must both be specified')

        zset = self._get_zset(name, "ZRANGEBYSCORE")

        if not zset:
            return []

        func = self._range_func(withscores, score_cast_func)

        scorerange = zset.scorerange(float(min_), float(max_))
        if start is not None and num is not None:
            start, num = self._translate_limit(len(scorerange), start, num)
            scorerange = scorerange[start:start + num]
        return [func(item) for item in scorerange]

    def zrank(self, name, value):
        zset = self._get_zset(name, "ZRANK")

        return zset.rank(value) if zset else None

    def zrem(self, name, *values):
        zset = self._get_zset(name, "ZREM")

        if not zset:
            return 0

        remove_count = lambda value: 1 if zset.remove(value) else 0
        return sum((remove_count(value) for value in values))

    def zremrangebyrank(self, name, start, end):
        zset = self._get_zset(name, "ZREMRANGEBYRANK")

        if not zset:
            return 0

        start, end = self._translate_range(len(zset), start, end)
        remove_count = lambda score, member: 1 if zset.remove(member) else 0
        return sum((remove_count(score, member) for score, member in zset.range(start, end)))

    def zremrangebyscore(self, name, min_, max_):
        zset = self._get_zset(name, "ZREMRANGEBYSCORE")

        if not zset:
            return 0

        remove_count = lambda score, member: 1 if zset.remove(member) else 0
        return sum((remove_count(score, member) for score, member in zset.scorerange(float(min_),
                                                                                     float(max_))))

    def zrevrange(self, name, start, end, withscores=False,
                  score_cast_func=float):
        return self.zrange(name, start, end,
                           desc=True, withscores=withscores, score_cast_func=score_cast_func)

    def zrevrangebyscore(self, name, max_, min_, start=None, num=None,
                         withscores=False, score_cast_func=float):
        if (start is None) ^ (num is None):
            raise TypeError('`start` and `num` must both be specified')

        zset = self._get_zset(name, "ZREVRANGEBYSCORE")
        if not zset:
            return []

        func = self._range_func(withscores, score_cast_func)

        scorerange = [x for x in reversed(zset.scorerange(float(min_), float(max_)))]
        if start is not None and num is not None:
            start, num = self._translate_limit(len(scorerange), start, num)
            scorerange = scorerange[start:start + num]
        return [func(item) for item in scorerange]

    def zrevrank(self, name, value):
        zset = self._get_zset(name, "ZREVRANK")

        if zset is None:
            return None

        return len(zset) - zset.rank(value) - 1

    def zscore(self, name, value):
        zset = self._get_zset(name, "ZSCORE")

        return zset.score(value) if zset is not None else None

    def zunionstore(self, dest, keys, aggregate=None):
        union = SortedSet()
        aggregate_func = self._aggregate_func(aggregate)

        for key in keys:
            zset = self._get_zset(key, "ZUNIONSTORE")
            if not zset:
                continue

            for score, member in zset:
                if member in union:
                    union[member] = aggregate_func(union[member], score)
                else:
                    union[member] = score

        # always override existing keys
        self.redis[dest] = union
        return len(union)

    def _get_zset(self, name, operation, create=False):
        """
        Get (and maybe create) a sorted set by name.
        """
        if name not in self.redis:
            if create:
                self.redis[name] = SortedSet()
        elif not isinstance(self.redis[name], SortedSet):
            raise TypeError("{} requires a sorted set".format(operation))
        return self.redis.get(name)

    def _translate_range(self, len_, start, end):
        """
        Translate range to valid bounds.
        """
        if start < 0:
            start = len_ + max(start, -len_)
        elif start > 0:
            start = min(start, len_)
        if end < 0:
            end = len_ + max(end, -(len_ + 1))
        elif end > 0:
            end = min(end, len_ - 1)
        return start, end

    def _translate_limit(self, len_, start, num):
        """
        Translate limit to valid bounds.
        """
        if start > len_ or num <= 0:
            return 0, 0
        return min(start, len_), num

    def _range_func(self, withscores, score_cast_func):
        """
        Return a suitable function from (score, member)
        """
        if withscores:
            return lambda (score, member): (member, score_cast_func(score))
        else:
            return lambda (score, member): member

    def _aggregate_func(self, aggregate):
        """
        Return a suitable aggregate score function.
        """
        if not aggregate or aggregate == 'sum':
            return add
        elif aggregate == 'min':
            return min
        elif aggregate == 'max':
            return max
        else:
            raise TypeError("Unsupported aggregate: {}".format(aggregate))


def mock_redis_client(**kwargs):
    """
    Mock common.util.redis_client so we
    can return a MockRedis object
    instead of a Redis object.
    """
    return MockRedis(**kwargs)
