import random
from datetime import datetime, timedelta
from collections import defaultdict

from mockredis.lock import MockRedisLock


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

    def __init__(self, **kwargs):
        pass

    def type(self, key):
        _type = type(self.redis[key])
        if _type == dict:
            return 'hash'
        elif _type == str:
            return 'string'
        elif _type == set:
            return 'set'
        elif _type == list:
            return 'list'
        return None

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

        if not key in self.redis:
            self.redis[key] = str(0 - decrement)
        self.redis[key] = str(long(self.redis[key]) - decrement)
        return long(self.redis[key])

    def incr(self, key, increment=1):

        if not key in self.redis:
            self.redis[key] = str(increment)
        self.redis[key] = str(long(self.redis[key]) + increment)
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

        if attribute in self.redis[key]:
            self.redis[key][attribute] = str(long(self.redis[key][attribute]) + increment)
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
        return -1 if key not in self.timeouts else (self.timeouts[key] - currenttime).total_seconds()

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
        rand_index = random.randint(0, length - 1)

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


def mock_redis_client(**kwargs):
    """
    Mock common.util.redis_client so we
    can return a MockRedis object
    instead of a Redis object.
    """
    return MockRedis(**kwargs)
