from collections import defaultdict
from datetime import datetime, timedelta
from operator import add
from random import choice, sample
from mockredis.lock import MockRedisLock
from mockredis.sortedset import SortedSet


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

    def __init__(self, strict=False, **kwargs):
        """
        Initialize as either StrictRedis or Redis.

        Defaults to non-strict.
        """
        self.strict = strict

    #### Connection Functions ####

    def echo(self, msg):
        return msg

    def ping(self):
        return "PONG"

    #### Transactions Functions ####

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

    def execute(self):
        """Emulate the execute method. All piped commands are executed immediately
        in this mock, so this is a no-op."""
        pass

    #### Keys Functions ####

    def type(self, key):
        if key not in self.redis:
            return 'none'
        type_ = type(self.redis[key])
        if type_ is dict:
            return 'hash'
        elif type_ is str:
            return 'string'
        elif type_ is set:
            return 'set'
        elif type_ is list:
            return 'list'
        elif type_ is SortedSet:
            return 'zset'
        raise TypeError("unhandled type {}".format(type_))

    def keys(self, pattern):
        """Emulate keys."""
        import re

        # Make a regex out of pattern. The only special matching character we look for is '*'
        regex = '^' + pattern.replace('*', '.*') + '$'

        # Find every key that matches the pattern
        result = [key for key in self.redis.keys() if re.match(regex, key)]

        return result

    def delete(self, key):
        """Emulate delete."""
        if key in self.redis:
            del self.redis[key]

    def exists(self, key):
        """Emulate exists."""

        return key in self.redis

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
        return -1 if key not in self.timeouts else self._get_total_seconds(self.timeouts[key] - currenttime)

    def do_expire(self, currenttime=datetime.now()):
        """
        Expire objects assuming now == time
        """
        for key, value in self.timeouts.items():
            if value - currenttime < timedelta(0):
                del self.timeouts[key]

    def flushdb(self):
        self.redis.clear()
        self.timeouts.clear()

    #### String Functions ####

    def get(self, key):

        # Override the default dict
        result = None if key not in self.redis else self.redis[key]
        return result

    def set(self, key, value):

        self.redis[key] = str(value)
        return True

    def decr(self, key, decrement=1):
        """Emulate decr."""

        previous_value = long(self.redis.get(key, '0'))
        self.redis[key] = str(previous_value - decrement)
        return long(self.redis[key])

    def incr(self, key, increment=1):

        previous_value = long(self.redis.get(key, '0'))
        self.redis[key] = str(previous_value + increment)
        return long(self.redis[key])

    #### Hash Functions ####

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
            self.redis[hashkey][attributekey] = str(attributevalue)

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

    #### List Functions ####

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

    def llen(self, key):
        """Emulate llen."""
        redis_list = self._get_list(key, 'LLEN')

        # Redis returns 0 if list doesn't exist
        return len(redis_list)

    def lpop(self, key):
        """Emulate lpop."""
        redis_list = self._get_list(key, 'LPOP')

        if key in self.redis:
            try:
                return str(redis_list.pop(0))
            except (IndexError):
                # Redis returns nil if popping from an empty list
                pass

    def lpush(self, key, *args):
        """Emulate lpush."""
        redis_list = self._get_list(key, 'LPUSH', create=True)

        # Creates the list at this key if it doesn't exist, and appends args to its beginning
        args_reversed = map(str, args)
        args_reversed.reverse()
        self.redis[key] = args_reversed + redis_list

    def rpop(self, key):
        """Emulate lpop."""
        redis_list = self._get_list(key, 'RPOP')

        if key in self.redis:
            try:
                return str(redis_list.pop())
            except (IndexError):
                # Redis returns nil if popping from an empty list
                pass

    def rpush(self, key, *args):
        """Emulate rpush."""
        redis_list = self._get_list(key, 'RPUSH', create=True)

        # Creates the list at this key if it doesn't exist, and appends args to it
        redis_list.extend(map(str, args))

    #### SET COMMANDS ####

    def sadd(self, key, *values):
        """Emulate sadd."""
        redis_set = self._get_set(key, 'SADD', create=True)
        before_count = len(redis_set)
        redis_set.update(map(str, values))
        after_count = len(redis_set)
        return after_count - before_count

    def scard(self, key):
        """Emulate scard."""
        redis_set = self._get_set(key, 'SADD')
        return len(redis_set)

    def sdiff(self, keys, *args):
        """Emulate sdiff."""
        func = lambda left, right: left.difference(right)
        return self._apply_to_sets(func, "SDIFF", keys, *args)

    def sdiffstore(self, dest, keys, *args):
        """Emulate sdiffstore."""
        result = self.sdiff(keys, *args)
        self.redis[dest] = result
        return len(result)

    def sinter(self, keys, *args):
        """Emulate sinter."""
        func = lambda left, right: left.intersection(right)
        return self._apply_to_sets(func, "SINTER", keys, *args)

    def sinterstore(self, dest, keys, *args):
        """Emulate sinterstore."""
        result = self.sinter(keys, *args)
        self.redis[dest] = result
        return len(result)

    def sismember(self, name, value):
        """Emulate sismember."""
        redis_set = self._get_set(name, 'SISMEMBER')
        if not redis_set:
            return 0
        return 1 if value in redis_set else 0

    def smembers(self, name):
        """Emulate smembers."""
        redis_set = self._get_set(name, 'SMEMBERS')
        return redis_set or set()

    def smove(self, src, dst, value):
        """Emulate smove."""
        src_set = self._get_set(src, 'SMOVE')
        dst_set = self._get_set(dst, 'SMOVE')

        if value not in src_set:
            return 0

        src_set.discard(value)
        dst_set.add(value)
        self.redis[src], self.redis[dst] = src_set, dst_set
        return 1

    def spop(self, name):
        """Emulate spop."""
        redis_set = self._get_set(name, 'SPOP')
        if not redis_set:
            return None
        member = choice(list(redis_set))
        redis_set.remove(member)
        return member

    def srandmember(self, name, number=None):
        """Emulate srandmember."""
        redis_set = self._get_set(name, 'SRANDMEMBER')
        if not redis_set:
            return None if number is None else []
        if number is None:
            return choice(list(redis_set))
        elif number > 0:
            return sample(list(redis_set), min(number, len(redis_set)))
        else:
            return [choice(list(redis_set)) for _ in xrange(abs(number))]

    def srem(self, key, *values):
        """Emulate srem."""
        redis_set = self._get_set(key, 'SREM')
        if not redis_set:
            return 0
        before_count = len(redis_set)
        for value in values:
            redis_set.discard(str(value))
        after_count = len(redis_set)
        return before_count - after_count

    def sunion(self, keys, *args):
        """Emulate sunion."""
        func = lambda left, right: left.union(right)
        return self._apply_to_sets(func, "SUNION", keys, *args)

    def sunionstore(self, dest, keys, *args):
        """Emulate sunionstore."""
        result = self.sunion(keys, *args)
        self.redis[dest] = result
        return len(result)

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

        insert_count = lambda member, score: 1 if zset.insert(str(member), float(score)) else 0
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

        value = str(value)
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
        zset = self._get_zset(name, "ZRANGE")

        if not zset:
            return []

        start, end = self._translate_range(len(zset), start, end)
        if start == len(zset) or end < start:
            return []

        func = self._range_func(withscores, score_cast_func)
        return [func(item) for item in zset.range(start, end, desc)]

    def zrangebyscore(self, name, min_, max_, start=None, num=None,
                      withscores=False, score_cast_func=float):
        if (start is None) ^ (num is None):
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
        return sum((remove_count(score, member)
                    for score, member in zset.scorerange(float(min_), float(max_))))

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

    #### Internal ####

    def _get_total_seconds(self, td):
        """
        For python 2.6 support
        """
        return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6

    def _get_list(self, key, operation, create=False):
        """
        Get (and maybe create) a list by name.
        """
        return self._get_by_type(key, operation, create, 'list', [])

    def _get_set(self, key, operation, create=False):
        """
        Get (and maybe create) a set by name.
        """
        return self._get_by_type(key, operation, create, 'set', set())

    def _get_zset(self, name, operation, create=False):
        """
        Get (and maybe create) a sorted set by name.
        """
        return self._get_by_type(name, operation, create, 'zset', SortedSet(), return_default=False)

    def _get_by_type(self, key, operation, create, type_, default, return_default=True):
        """
        Get (and maybe create) a redis data structure by name and type.
        """
        if self.type(key) in [type_, 'none']:
            if create:
                return self.redis.setdefault(key, default)
            else:
                return self.redis.get(key, default if return_default else None)

        raise TypeError("{} requires a {}".format(operation, type_))

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

    def _apply_to_sets(self, func, operation, keys, *args):
        """Helper function for sdiff, sinter, and sunion"""
        keys = self._list_or_args(keys, args)
        if not keys:
            raise ValueError("wrong number of arguments for '{}' command".format(operation.lower()))
        left = self._get_set(keys[0], operation) or set()
        for key in keys[1:]:
            right = self._get_set(key, operation) or set()
            left = func(left, right)
        return left

    def _list_or_args(self, keys, args):
        """
        Shamelessly copied from redis-py.
        """
        # returns a single list combining keys and args
        try:
            iter(keys)
            # a string can be iterated, but indicates
            # keys wasn't passed as a list
            if isinstance(keys, basestring):
                keys = [keys]
        except TypeError:
            keys = [keys]
        if args:
            keys.extend(args)
        return keys


def mock_redis_client(**kwargs):
    """
    Mock common.util.redis_client so we
    can return a MockRedis object
    instead of a Redis object.
    """
    return MockRedis()


def mock_strict_redis_client(**kwargs):
    """
    Mock common.util.redis_client so we
    can return a MockRedis object
    instead of a StrictRedis object.
    """
    return MockRedis(strict=True)
