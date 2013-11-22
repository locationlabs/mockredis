from collections import defaultdict
from datetime import datetime, timedelta
from hashlib import sha1
from operator import add
from random import choice, sample
import string

from mockredis.lock import MockRedisLock
from mockredis.exceptions import RedisError
from mockredis.pipeline import MockRedisPipeline
from mockredis.script import Script
from mockredis.sortedset import SortedSet


class MockRedis(object):
    """
    A Mock for a redis-py Redis object

    Expire functionality must be explicitly
    invoked using do_expire(time). Automatic
    expiry is NOT supported.
    """

    def __init__(self, strict=False, **kwargs):
        """
        Initialize as either StrictRedis or Redis.

        Defaults to non-strict.
        """
        self.strict = strict
        # The 'Redis' store
        self.redis = defaultdict(dict)
        self.timeouts = defaultdict(dict)
        # Dictionary from script to sha ''Script''
        self.shas = dict()

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
        return MockRedisPipeline(self)

    def watch(self, *argv, **kwargs):
        """
        Mock does not support command buffering so watch
        is a no-op
        """
        pass

    def unwatch(self):
        """
        Mock does not support command buffering so unwatch
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

    def delete(self, *keys):
        """Emulate delete."""

        key_counter = 0
        for key in keys:
            if key in self.redis:
                del self.redis[key]
                key_counter += 1
            if key in self.timeouts:
                del self.timeouts[key]
        return key_counter

    def exists(self, key):
        """Emulate exists."""

        return key in self.redis

    def _expire(self, key, delta, currenttime=datetime.now()):
        if key not in self.redis:
            return False

        self.timeouts[key] = currenttime + delta
        return True

    def expire(self, key, seconds, currenttime=datetime.now()):
        """Emulate expire"""
        return self._expire(key, timedelta(seconds=seconds), currenttime)

    def pexpire(self, key, milliseconds, currenttime=datetime.now()):
        """Emulate pexpire"""
        return self._expire(key, timedelta(milliseconds=milliseconds), currenttime)

    def expireat(self, key, when):
        """Emulate expireat"""
        expire_time = datetime.fromtimestamp(when)
        if key in self.redis:
            self.timeouts[key] = expire_time
            return True
        return False

    def _time_to_live(self, key, output_ms, currenttime=datetime.now()):
        """
        Returns time to live in milliseconds if output_ms is True, else returns seconds.
        """
        if key not in self.timeouts:
            return None

        get_result = self._get_total_milliseconds if output_ms else self._get_total_seconds
        time_to_live = get_result(self.timeouts[key] - currenttime)
        return max(-1, time_to_live)

    def ttl(self, key, currenttime=datetime.now()):
        """
        Emulate ttl

        Even though the official redis commands documentation at http://redis.io/commands/ttl
        states "Return value: Integer reply: TTL in seconds, -2 when key does not exist or -1
        when key does not have a timeout." the redis-py lib returns None for both these cases.
        The lib behavior has been emulated here.

        :param key: key for which ttl is requested.
        :returns: the number of seconds till timeout, None if the key does not exist or if the
                  key has no timeout(as per the redis-py lib behavior).
        """
        return self._time_to_live(key, output_ms=False, currenttime=currenttime)

    def pttl(self, key, currenttime=datetime.now()):
        """
        Emulate pttl

        :param key: key for which pttl is requested.
        :returns: the number of milliseconds till timeout, None if the key does not exist or if the
                  key has no timeout(as per the redis-py lib behavior).
        """
        return self._time_to_live(key, output_ms=True, currenttime=currenttime)

    def do_expire(self, currenttime=datetime.now()):
        """
        Expire objects assuming now == time
        """
        for key, value in self.timeouts.items():
            if value - currenttime < timedelta(0):
                del self.timeouts[key]
                # removing the expired key
                if key in self.redis:
                    self.redis.pop(key, None)

    def flushdb(self):
        self.redis.clear()
        self.timeouts.clear()

    #### String Functions ####

    def get(self, key):

        # Override the default dict
        result = None if key not in self.redis else self.redis[key]
        return result

    def set(self, key, value, ex=None, px=None, nx=False, xx=False, currenttime=datetime.now()):
        """
        Set the ``value`` for the ``key`` in the context of the provided kwargs.

        As per the behavior of the redis-py lib:
        If nx and xx are both set, the function does nothing and None is returned.
        If px and ex are both set, the preference is given to px.
        If the key is not set for some reason, the lib function returns None.

        """

        if nx and xx:
            return None
        mode = "nx" if nx else "xx" if xx else None
        if self._should_set(key, mode):
            expire = None
            if ex is not None:
                expire = ex if isinstance(ex, timedelta) else timedelta(seconds=ex)
            if px is not None:
                expire = px if isinstance(px, timedelta) else timedelta(milliseconds=px)

            if expire is not None and expire.total_seconds() <= 0:
                raise ValueError("invalid expire time in SETEX")

            result = self._set(key, value)
            if expire:
                self._expire(key, expire, currenttime=currenttime)

            return result

    def _set(self, key, value):
        self.redis[key] = str(value)

        # removing the timeout
        if key in self.timeouts:
            self.timeouts.pop(key, None)

        return True

    def _should_set(self, key, mode):
        """
        Determine if it is okay to set a key.

        If the mode is None, returns True, otherwise, returns True of false based on
        the value of ``key`` and the ``mode`` (nx | xx).
        """

        if mode is None or mode not in ["nx", "xx"]:
            return True

        if mode == "nx":
            if key in self.redis:
                # nx means set only if key is absent
                # false if the key already exists
                return False
        elif key not in self.redis:
            # at this point mode can only be xx
            # xx means set only if the key already exists
            # false if is absent
            return False
        # for all other cases, return true
        return True

    def setex(self, key, time, value, currenttime=datetime.now()):
        """
        Set the value of ``key`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        return self.set(key, value, ex=time, currenttime=currenttime)

    def psetex(self, key, time, value, currenttime=datetime.now()):
        """
        Set the value of ``key`` to ``value`` that expires in ``time``
        milliseconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        return self.set(key, value, px=time, currenttime=currenttime)

    def setnx(self, key, value):
        """Set the value of ``key`` to ``value`` if key doesn't exist"""
        return self.set(key, value, nx=True)

    def decr(self, key, amount=1):
        """Emulate decr."""
        previous_value = long(self.redis.get(key, '0'))
        self.redis[key] = str(previous_value - amount)
        return long(self.redis[key])

    def decrby(self, key, amount=1):
        return self.decr(key, amount)

    def incr(self, key, amount=1):
        """Emulate incr."""
        previous_value = long(self.redis.get(key, '0'))
        self.redis[key] = str(previous_value + amount)
        return long(self.redis[key])

    def incrby(self, key, amount=1):
        return self.incr(key, amount)

    #### Hash Functions ####

    def hexists(self, hashkey, attribute):
        """Emulate hexists."""

        redis_hash = self._get_hash(hashkey, 'HEXISTS')
        return str(attribute) in redis_hash

    def hget(self, hashkey, attribute):
        """Emulate hget."""

        redis_hash = self._get_hash(hashkey, 'HGET')
        return redis_hash.get(str(attribute))

    def hgetall(self, hashkey):
        """Emulate hgetall."""

        redis_hash = self._get_hash(hashkey, 'HGETALL')
        return dict(redis_hash)

    def hdel(self, hashkey, *keys):
        """Emulate hdel"""

        redis_hash = self._get_hash(hashkey, 'HDEL')
        count = 0
        for key in keys:
            attribute = str(key)
            if attribute in redis_hash:
                count += 1
                del redis_hash[attribute]
                if not redis_hash:
                    del self.redis[hashkey]
        return count

    def hlen(self, hashkey):
        """Emulate hlen."""
        redis_hash = self._get_hash(hashkey, 'HLEN')
        return len(redis_hash)

    def hmset(self, hashkey, value):
        """Emulate hmset."""

        redis_hash = self._get_hash(hashkey, 'HMSET', create=True)
        for key, value in value.items():
            attribute = str(key)
            redis_hash[attribute] = str(value)

    def hmget(self, hashkey, keys, *args):
        """Emulate hmget."""

        redis_hash = self._get_hash(hashkey, 'HMGET')
        attributes = self._list_or_args(keys, args)
        return [redis_hash.get(str(attribute)) for attribute in attributes]

    def hset(self, hashkey, attribute, value):
        """Emulate hset."""

        redis_hash = self._get_hash(hashkey, 'HSET', create=True)
        attribute = str(attribute)
        redis_hash[attribute] = str(value)

    def hsetnx(self, hashkey, attribute, value):
        """Emulate hsetnx."""

        redis_hash = self._get_hash(hashkey, 'HSETNX', create=True)
        attribute = str(attribute)
        if attribute in redis_hash:
            return 0
        else:
            redis_hash[attribute] = str(value)
            return 1

    def hincrby(self, hashkey, attribute, increment=1):
        """Emulate hincrby."""

        return self._hincrby(hashkey, attribute, 'HINCRBY', long, increment)

    def hincrbyfloat(self, hashkey, attribute, increment=1.0):
        """Emulate hincrbyfloat."""

        return self._hincrby(hashkey, attribute, 'HINCRBYFLOAT', float, increment)

    def _hincrby(self, hashkey, attribute, command, type_, increment):
        """Shared hincrby and hincrbyfloat routine"""
        redis_hash = self._get_hash(hashkey, command, create=True)
        attribute = str(attribute)
        previous_value = type_(redis_hash.get(attribute, '0'))
        redis_hash[attribute] = str(previous_value + increment)
        return type_(redis_hash[attribute])

    def hkeys(self, hashkey):
        """Emulate hkeys."""

        redis_hash = self._get_hash(hashkey, 'HKEYS')
        return redis_hash.keys()

    def hvals(self, hashkey):
        """Emulate hvals."""

        redis_hash = self._get_hash(hashkey, 'HVALS')
        return redis_hash.values()

    #### List Functions ####

    def lrange(self, key, start, stop):
        """Emulate lrange."""
        redis_list = self._get_list(key, 'LRANGE')
        start, stop = self._translate_range(len(redis_list), start, stop)
        return redis_list[start:stop + 1]

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

    def lrem(self, key, value, count=0):
        """Emulate lrem."""
        redis_list = self._get_list(key, 'LREM')
        removed_count = 0
        if key in self.redis:
            if count == 0:
                # Remove all ocurrences
                while redis_list.count(value):
                    redis_list.remove(value)
                    removed_count += 1
            elif count > 0:
                counter = 0
                # remove first 'count' ocurrences
                while redis_list.count(value):
                    redis_list.remove(value)
                    counter += 1
                    removed_count += 1
                    if counter >= count:
                        break
            elif count < 0:
                # remove last 'count' ocurrences
                counter = -count
                new_list = []
                for v in reversed(redis_list):
                    if v == value and counter > 0:
                        counter -= 1
                        removed_count += 1
                    else:
                        new_list.append(v)
                self.redis[key] = list(reversed(new_list))
        return removed_count

    def ltrim(self, key, start, stop):
        """Emulate ltrim."""
        redis_list = self._get_list(key, 'LTRIM')
        if redis_list:
            start, stop = self._translate_range(len(redis_list), start, stop)
            self.redis[key] = redis_list[start:stop + 1]
        return True

    def rpoplpush(self, source, destination):
        """Emulate rpoplpush"""
        transfer_item = self.rpop(source)
        self.lpush(destination, transfer_item)
        return transfer_item

    def lset(self, key, index, value):
        """Emulate lset."""
        redis_list = self._get_list(key, 'LSET')
        if redis_list is None:
            raise ValueError("no such key")
        try:
            redis_list[index] = value
        except IndexError:
            raise ValueError("index out of range")

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
            return False
        return str(value) in redis_set

    def smembers(self, name):
        """Emulate smembers."""
        redis_set = self._get_set(name, 'SMEMBERS')
        return redis_set or set()

    def smove(self, src, dst, value):
        """Emulate smove."""
        src_set = self._get_set(src, 'SMOVE')
        dst_set = self._get_set(dst, 'SMOVE')

        if value not in src_set:
            return False

        src_set.discard(value)
        dst_set.add(value)
        self.redis[src], self.redis[dst] = src_set, dst_set
        return True

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

    #### Script Commands ####

    def eval(self, script, numkeys, *keys_and_args):
        """Emulate eval"""
        sha = self.script_load(script)
        return self.evalsha(sha, numkeys, *keys_and_args)

    def evalsha(self, sha, numkeys, *keys_and_args):
        """Emulates evalsha"""
        if not self.script_exists(sha)[0]:
            raise RedisError("Sha not registered")
        script_callable = Script(self, self.shas[sha])
        numkeys = max(numkeys, 0)
        keys = keys_and_args[:numkeys]
        args = keys_and_args[numkeys:]
        return script_callable(keys, args)

    def script_exists(self, *args):
        """Emulates script_exists"""
        return [arg in self.shas for arg in args]

    def script_flush(self):
        """Emulate script_flush"""
        self.shas.clear()

    def script_kill(self):
        """Emulate script_kill"""
        """XXX: To be implemented, should not be called before that."""
        raise NotImplementedError("Not yet implemented.")

    def script_load(self, script):
        """Emulate script_load"""
        sha_digest = sha1(script).hexdigest()
        self.shas[sha_digest] = script
        return sha_digest

    def register_script(self, script):
        """Emulate register_script"""
        return Script(self, script)

    def call(self, command, *args):
        """
        Sends call to the function, whose name is specified by command.
        """
        command = self._normalize_command_name(command)
        args = self._normalize_command_args(command, *args)

        redis_function = getattr(self, command)
        value = redis_function(*args)
        return value

    def _normalize_command_name(self, command):
        """
        Modifies the command string to match the redis client method name.
        """
        command = string.lower(command)

        if command == 'del':
            return 'delete'

        return command

    def _normalize_command_args(self, command, *args):
        """
        Modifies the command arguments to match the
        strictness of the redis client.
        """
        if command == 'zadd' and not self.strict and len(args) >= 3:
            # Reorder score and name
            zadd_args = [x for tup in zip(args[2::2], args[1::2]) for x in tup]
            return [args[0]] + zadd_args

        if command == 'zrangebyscore' and len(args) == 6:
            # Remove 'limit' from arguments
            return args[:3] + args[4:]

        return args

    #### Internal ####

    def _get_total_seconds(self, td):
        """
        For python 2.6 support
        """
        return int((td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6)

    def _get_total_milliseconds(self, td):
        return int((td.days * 24 * 60 * 60 + td.seconds) * 1000 + td.microseconds / 1000.0)

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

    def _get_hash(self, name, operation, create=False):
        """
        Get (and maybe create) a hash by name.
        """
        return self._get_by_type(name, operation, create, 'hash', {})

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
            start += len_
        start = max(0, min(start, len_))
        if end < 0:
            end += len_
        end = max(-1, min(end, len_ - 1))
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
        funcs = {"sum": add, "min": min, "max": max}
        func_name = aggregate.lower() if aggregate else 'sum'
        try:
            return funcs[func_name]
        except KeyError:
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
