from __future__ import division
from collections import defaultdict
from copy import deepcopy
from itertools import chain
from datetime import datetime, timedelta
from hashlib import sha1
from operator import add
from random import choice, sample
import re
import sys
import time
import fnmatch

from mockredis.clock import SystemClock
from mockredis.lock import MockRedisLock
from mockredis.exceptions import RedisError, ResponseError, WatchError
from mockredis.pipeline import MockRedisPipeline
from mockredis.script import Script
from mockredis.sortedset import SortedSet

if sys.version_info >= (3, 0):
    long = int
    xrange = range
    basestring = str
    from functools import reduce


class MockRedis(object):
    """
    A Mock for a redis-py Redis object

    Expire functionality must be explicitly
    invoked using do_expire(time). Automatic
    expiry is NOT supported.
    """

    def __init__(self,
                 strict=False,
                 clock=None,
                 load_lua_dependencies=True,
                 blocking_timeout=1000,
                 blocking_sleep_interval=0.01,
                 **kwargs):
        """
        Initialize as either StrictRedis or Redis.

        Defaults to non-strict.
        """
        self.strict = strict
        self.clock = SystemClock() if clock is None else clock
        self.load_lua_dependencies = load_lua_dependencies
        self.blocking_timeout = blocking_timeout
        self.blocking_sleep_interval = blocking_sleep_interval
        # The 'Redis' store
        self.redis = defaultdict(dict)
        self.redis_config = defaultdict(dict)
        self.timeouts = defaultdict(dict)
        # The 'PubSub' store
        self.pubsub = defaultdict(list)
        # Dictionary from script to sha ''Script''
        self.shas = dict()

    @classmethod
    def from_url(cls, url, db=None, **kwargs):
        return cls(**kwargs)

    # Connection Functions #

    def echo(self, msg):
        return self._encode(msg)

    def ping(self):
        return b'PONG'

    # Transactions Functions #

    def lock(self, key, timeout=0, sleep=0):
        """Emulate lock."""
        return MockRedisLock(self, key, timeout, sleep)

    def pipeline(self, transaction=True, shard_hint=None):
        """Emulate a redis-python pipeline."""
        return MockRedisPipeline(self, transaction, shard_hint)

    def transaction(self, func, *watches, **kwargs):
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single argument which is a Pipeline object.

        Copied directly from redis-py.
        """
        shard_hint = kwargs.pop('shard_hint', None)
        value_from_callable = kwargs.pop('value_from_callable', False)
        watch_delay = kwargs.pop('watch_delay', None)
        with self.pipeline(True, shard_hint) as pipe:
            while 1:
                try:
                    if watches:
                        pipe.watch(*watches)
                    func_value = func(pipe)
                    exec_value = pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        time.sleep(watch_delay)
                    continue

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

    # Keys Functions #

    def type(self, key):
        key = self._encode(key)
        if key not in self.redis:
            return b'none'
        type_ = type(self.redis[key])
        if type_ is dict:
            return b'hash'
        elif type_ is str:
            return b'string'
        elif type_ is set:
            return b'set'
        elif type_ is list:
            return b'list'
        elif type_ is SortedSet:
            return b'zset'
        raise TypeError("unhandled type {}".format(type_))

    def keys(self, pattern='*'):
        """Emulate keys."""
        # making sure the pattern is unicode/str.
        try:
            pattern = pattern.decode('utf-8')
            # This throws an AttributeError in python 3, or an
            # UnicodeEncodeError in python 2
        except (AttributeError, UnicodeEncodeError):
            pass

        # Make regex out of glob styled pattern.
        regex = fnmatch.translate(pattern)
        regex = re.compile(re.sub(r'(^|[^\\])\.', r'\1[^/]', regex))

        # Find every key that matches the pattern
        return [key for key in self.redis.keys() if regex.match(key.decode('utf-8'))]

    def delete(self, *keys):
        """Emulate delete."""
        key_counter = 0
        for key in map(self._encode, keys):
            if key in self.redis:
                del self.redis[key]
                key_counter += 1
            if key in self.timeouts:
                del self.timeouts[key]
        return key_counter

    def __delitem__(self, name):
        if self.delete(name) == 0:
            # redispy doesn't correctly raise KeyError here, so we don't either
            pass

    def exists(self, key):
        """Emulate exists."""
        return self._encode(key) in self.redis
    __contains__ = exists

    def _expire(self, key, delta):
        if key not in self.redis:
            return False

        self.timeouts[key] = self.clock.now() + delta
        return True

    def expire(self, key, delta):
        """Emulate expire"""
        delta = delta if isinstance(delta, timedelta) else timedelta(seconds=delta)
        return self._expire(self._encode(key), delta)

    def pexpire(self, key, milliseconds):
        """Emulate pexpire"""
        return self._expire(self._encode(key), timedelta(milliseconds=milliseconds))

    def expireat(self, key, when):
        """Emulate expireat"""
        expire_time = datetime.fromtimestamp(when)
        key = self._encode(key)
        if key in self.redis:
            self.timeouts[key] = expire_time
            return True
        return False

    def ttl(self, key):
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
        value = self.pttl(key)
        if value is None or value < 0:
            return value
        return value // 1000

    def pttl(self, key):
        """
        Emulate pttl

        :param key: key for which pttl is requested.
        :returns: the number of milliseconds till timeout, None if the key does not exist or if the
                  key has no timeout(as per the redis-py lib behavior).
        """
        """
        Returns time to live in milliseconds if output_ms is True, else returns seconds.
        """
        key = self._encode(key)
        if key not in self.redis:
            # as of redis 2.8, -2 is returned if the key does not exist
            return long(-2) if self.strict else None
        if key not in self.timeouts:
            # as of redis 2.8, -1 is returned if the key is persistent
            # redis-py returns None; command docs say -1
            return long(-1) if self.strict else None

        time_to_live = get_total_milliseconds(self.timeouts[key] - self.clock.now())
        return long(max(-1, time_to_live))

    def do_expire(self):
        """
        Expire objects assuming now == time
        """
        # Deep copy to avoid RuntimeError: dictionary changed size during iteration
        _timeouts = deepcopy(self.timeouts)
        for key, value in _timeouts.items():
            if value - self.clock.now() < timedelta(0):
                del self.timeouts[key]
                # removing the expired key
                if key in self.redis:
                    self.redis.pop(key, None)

    def flushdb(self):
        self.redis.clear()
        self.pubsub.clear()
        self.timeouts.clear()

    def rename(self, old_key, new_key):
        return self._rename(old_key, new_key)

    def renamenx(self, old_key, new_key):
        return 1 if self._rename(old_key, new_key, True) else 0

    def _rename(self, old_key, new_key, nx=False):
        old_key = self._encode(old_key)
        new_key = self._encode(new_key)
        if old_key in self.redis and (not nx or new_key not in self.redis):
            self.redis[new_key] = self.redis.pop(old_key)
            return True
        return False

    def dbsize(self):
        return len(self.redis.keys())

    # String Functions #

    def get(self, key):
        key = self._encode(key)
        return self.redis.get(key)

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value is not None:
            return value
        raise KeyError(name)

    def mget(self, keys, *args):
        args = self._list_or_args(keys, args)
        return [self.get(arg) for arg in args]

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """
        Set the ``value`` for the ``key`` in the context of the provided kwargs.

        As per the behavior of the redis-py lib:
        If nx and xx are both set, the function does nothing and None is returned.
        If px and ex are both set, the preference is given to px.
        If the key is not set for some reason, the lib function returns None.
        """
        key = self._encode(key)
        value = self._encode(value)

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
                raise ResponseError("invalid expire time in SETEX")

            result = self._set(key, value)
            if expire:
                self._expire(key, expire)

            return result
    __setitem__ = set

    def getset(self, key, value):
        old_value = self.get(key)
        self.set(key, value)
        return old_value

    def _set(self, key, value):
        self.redis[key] = self._encode(value)

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

    def setex(self, key, time, value):
        """
        Set the value of ``key`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        if not self.strict:
            # when not strict mode swap value and time args order
            time, value = value, time
        return self.set(key, value, ex=time)

    def psetex(self, key, time, value):
        """
        Set the value of ``key`` to ``value`` that expires in ``time``
        milliseconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        return self.set(key, value, px=time)

    def setnx(self, key, value):
        """Set the value of ``key`` to ``value`` if key doesn't exist"""
        return self.set(key, value, nx=True)

    def mset(self, *args, **kwargs):
        """
        Sets key/values based on a mapping. Mapping can be supplied as a single
        dictionary argument or as kwargs.
        """
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise RedisError('MSET requires **kwargs or a single dict arg')
            mapping = args[0]
        else:
            mapping = kwargs
        for key, value in mapping.items():
            self.set(key, value)
        return True

    def msetnx(self, *args, **kwargs):
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping can be supplied as a single dictionary argument or as kwargs.
        Returns a boolean indicating if the operation was successful.
        """
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise RedisError('MSETNX requires **kwargs or a single dict arg')
            mapping = args[0]
        else:
            mapping = kwargs

        for key in mapping.keys():
            if self._encode(key) in self.redis:
                return False
        for key, value in mapping.items():
            self.set(key, value)

        return True

    def decr(self, key, amount=1):
        key = self._encode(key)
        previous_value = long(self.redis.get(key, '0'))
        self.redis[key] = self._encode(previous_value - amount)
        return long(self.redis[key])

    decrby = decr

    def incr(self, key, amount=1):
        """Emulate incr."""
        key = self._encode(key)
        previous_value = long(self.redis.get(key, '0'))
        self.redis[key] = self._encode(previous_value + amount)
        return long(self.redis[key])

    incrby = incr

    def setbit(self, key, offset, value):
        """
        Set the bit at ``offset`` in ``key`` to ``value``.
        """
        key = self._encode(key)
        index, bits, mask = self._get_bits_and_offset(key, offset)

        if index >= len(bits):
            bits.extend(b"\x00" * (index + 1 - len(bits)))

        prev_val = 1 if (bits[index] & mask) else 0

        if value:
            bits[index] |= mask
        else:
            bits[index] &= ~mask

        self.redis[key] = bytes(bits)

        return prev_val

    def getbit(self, key, offset):
        """
        Returns the bit value at ``offset`` in ``key``.
        """
        key = self._encode(key)
        index, bits, mask = self._get_bits_and_offset(key, offset)

        if index >= len(bits):
            return 0

        return 1 if (bits[index] & mask) else 0

    def _get_bits_and_offset(self, key, offset):
        bits = bytearray(self.redis.get(key, b""))
        index, position = divmod(offset, 8)
        mask = 128 >> position
        return index, bits, mask

    # Hash Functions #

    def hexists(self, hashkey, attribute):
        """Emulate hexists."""

        redis_hash = self._get_hash(hashkey, 'HEXISTS')
        return self._encode(attribute) in redis_hash

    def hget(self, hashkey, attribute):
        """Emulate hget."""

        redis_hash = self._get_hash(hashkey, 'HGET')
        return redis_hash.get(self._encode(attribute))

    def hgetall(self, hashkey):
        """Emulate hgetall."""

        redis_hash = self._get_hash(hashkey, 'HGETALL')
        return dict(redis_hash)

    def hdel(self, hashkey, *keys):
        """Emulate hdel"""

        redis_hash = self._get_hash(hashkey, 'HDEL')
        count = 0
        for key in keys:
            attribute = self._encode(key)
            if attribute in redis_hash:
                count += 1
                del redis_hash[attribute]
                if not redis_hash:
                    self.delete(hashkey)
        return count

    def hlen(self, hashkey):
        """Emulate hlen."""
        redis_hash = self._get_hash(hashkey, 'HLEN')
        return len(redis_hash)

    def hmset(self, hashkey, value):
        """Emulate hmset."""

        redis_hash = self._get_hash(hashkey, 'HMSET', create=True)
        for key, value in value.items():
            attribute = self._encode(key)
            redis_hash[attribute] = self._encode(value)
        return True

    def hmget(self, hashkey, keys, *args):
        """Emulate hmget."""

        redis_hash = self._get_hash(hashkey, 'HMGET')
        attributes = self._list_or_args(keys, args)
        return [redis_hash.get(self._encode(attribute)) for attribute in attributes]

    def hset(self, hashkey, attribute, value):
        """Emulate hset."""

        redis_hash = self._get_hash(hashkey, 'HSET', create=True)
        attribute = self._encode(attribute)
        attribute_present = attribute in redis_hash
        redis_hash[attribute] = self._encode(value)
        return long(0) if attribute_present else long(1)

    def hsetnx(self, hashkey, attribute, value):
        """Emulate hsetnx."""

        redis_hash = self._get_hash(hashkey, 'HSETNX', create=True)
        attribute = self._encode(attribute)
        if attribute in redis_hash:
            return long(0)
        else:
            redis_hash[attribute] = self._encode(value)
            return long(1)

    def hincrby(self, hashkey, attribute, increment=1):
        """Emulate hincrby."""

        return self._hincrby(hashkey, attribute, 'HINCRBY', long, increment)

    def hincrbyfloat(self, hashkey, attribute, increment=1.0):
        """Emulate hincrbyfloat."""

        return self._hincrby(hashkey, attribute, 'HINCRBYFLOAT', float, increment)

    def _hincrby(self, hashkey, attribute, command, type_, increment):
        """Shared hincrby and hincrbyfloat routine"""
        redis_hash = self._get_hash(hashkey, command, create=True)
        attribute = self._encode(attribute)
        previous_value = type_(redis_hash.get(attribute, '0'))
        redis_hash[attribute] = self._encode(previous_value + increment)
        return type_(redis_hash[attribute])

    def hkeys(self, hashkey):
        """Emulate hkeys."""

        redis_hash = self._get_hash(hashkey, 'HKEYS')
        return redis_hash.keys()

    def hvals(self, hashkey):
        """Emulate hvals."""

        redis_hash = self._get_hash(hashkey, 'HVALS')
        return redis_hash.values()

    # List Functions #

    def lrange(self, key, start, stop):
        """Emulate lrange."""
        redis_list = self._get_list(key, 'LRANGE')
        start, stop = self._translate_range(len(redis_list), start, stop)
        return redis_list[start:stop + 1]

    def lindex(self, key, index):
        """Emulate lindex."""

        redis_list = self._get_list(key, 'LINDEX')

        if self._encode(key) not in self.redis:
            return None

        try:
            return redis_list[index]
        except (IndexError):
            # Redis returns nil if the index doesn't exist
            return None

    def llen(self, key):
        """Emulate llen."""
        redis_list = self._get_list(key, 'LLEN')

        # Redis returns 0 if list doesn't exist
        return len(redis_list)

    def _blocking_pop(self, pop_func, keys, timeout):
        """Emulate blocking pop functionality"""
        if not isinstance(timeout, (int, long)):
            raise RuntimeError('timeout is not an integer or out of range')

        if timeout is None or timeout == 0:
            timeout = self.blocking_timeout

        if isinstance(keys, basestring):
            keys = [keys]
        else:
            keys = list(keys)

        elapsed_time = 0
        start = time.time()
        while elapsed_time < timeout:
            key, val = self._pop_first_available(pop_func, keys)
            if val:
                return key, val
            # small delay to avoid high cpu utilization
            time.sleep(self.blocking_sleep_interval)
            elapsed_time = time.time() - start
        return None

    def _pop_first_available(self, pop_func, keys):
        for key in keys:
            val = pop_func(key)
            if val:
                return self._encode(key), val
        return None, None

    def blpop(self, keys, timeout=0):
        """Emulate blpop"""
        return self._blocking_pop(self.lpop, keys, timeout)

    def brpop(self, keys, timeout=0):
        """Emulate brpop"""
        return self._blocking_pop(self.rpop, keys, timeout)

    def lpop(self, key):
        """Emulate lpop."""
        redis_list = self._get_list(key, 'LPOP')

        if self._encode(key) not in self.redis:
            return None

        try:
            value = redis_list.pop(0)
            if len(redis_list) == 0:
                self.delete(key)
            return value
        except (IndexError):
            # Redis returns nil if popping from an empty list
            return None

    def lpush(self, key, *args):
        """Emulate lpush."""
        redis_list = self._get_list(key, 'LPUSH', create=True)

        # Creates the list at this key if it doesn't exist, and appends args to its beginning
        args_reversed = [self._encode(arg) for arg in args]
        args_reversed.reverse()
        updated_list = args_reversed + redis_list
        self.redis[self._encode(key)] = updated_list

        # Return the length of the list after the push operation
        return len(updated_list)

    def rpop(self, key):
        """Emulate lpop."""
        redis_list = self._get_list(key, 'RPOP')

        if self._encode(key) not in self.redis:
            return None

        try:
            value = redis_list.pop()
            if len(redis_list) == 0:
                self.delete(key)
            return value
        except (IndexError):
            # Redis returns nil if popping from an empty list
            return None

    def rpush(self, key, *args):
        """Emulate rpush."""
        redis_list = self._get_list(key, 'RPUSH', create=True)

        # Creates the list at this key if it doesn't exist, and appends args to it
        redis_list.extend(map(self._encode, args))

        # Return the length of the list after the push operation
        return len(redis_list)

    def lrem(self, key, value, count=0):
        """Emulate lrem."""
        value = self._encode(value)
        redis_list = self._get_list(key, 'LREM')
        removed_count = 0
        if self._encode(key) in self.redis:
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
                redis_list[:] = list(reversed(new_list))
        if removed_count > 0 and len(redis_list) == 0:
            self.delete(key)
        return removed_count

    def ltrim(self, key, start, stop):
        """Emulate ltrim."""
        redis_list = self._get_list(key, 'LTRIM')
        if redis_list:
            start, stop = self._translate_range(len(redis_list), start, stop)
            self.redis[self._encode(key)] = redis_list[start:stop + 1]
        return True

    def rpoplpush(self, source, destination):
        """Emulate rpoplpush"""
        transfer_item = self.rpop(source)
        if transfer_item is not None:
            self.lpush(destination, transfer_item)
        return transfer_item

    def brpoplpush(self, source, destination, timeout=0):
        """Emulate brpoplpush"""
        transfer_item = self.brpop(source, timeout)
        if transfer_item is None:
            return None

        key, val = transfer_item
        self.lpush(destination, val)
        return val

    def lset(self, key, index, value):
        """Emulate lset."""
        redis_list = self._get_list(key, 'LSET')
        if redis_list is None:
            raise ResponseError("no such key")
        try:
            redis_list[index] = self._encode(value)
        except IndexError:
            raise ResponseError("index out of range")

    def sort(self, name,
             start=None,
             num=None,
             by=None,
             get=None,
             desc=False,
             alpha=False,
             store=None,
             groups=False):
        # check valid parameter combos
        if [start, num] != [None, None] and None in [start, num]:
            raise ValueError('start and num must both be specified together')

        # check up-front if there's anything to actually do
        items = num != 0 and self.get(name)
        if not items:
            if store:
                return 0
            else:
                return []

        by = self._encode(by) if by is not None else by
        # always organize the items as tuples of the value from the list and the sort key
        if by and b'*' in by:
            items = [(i, self.get(by.replace(b'*', self._encode(i)))) for i in items]
        elif by in [None, b'nosort']:
            items = [(i, i) for i in items]
        else:
            raise ValueError('invalid value for "by": %s' % by)

        if by != b'nosort':
            # if sorting, do alpha sort or float (default) and take desc flag into account
            sort_type = alpha and str or float
            items.sort(key=lambda x: sort_type(x[1]), reverse=bool(desc))

        # results is a list of lists to support different styles of get and also groups
        results = []
        if get:
            if isinstance(get, basestring):
                # always deal with get specifiers as a list
                get = [get]
            for g in map(self._encode, get):
                if g == b'#':
                    results.append([self.get(i) for i in items])
                else:
                    results.append([self.get(g.replace(b'*', self._encode(i[0]))) for i in items])
        else:
            # if not using GET then returning just the item itself
            results.append([i[0] for i in items])

        # results to either list of tuples or list of values
        if len(results) > 1:
            results = list(zip(*results))
        elif results:
            results = results[0]

        # apply the 'start' and 'num' to the results
        if not start:
            start = 0
        if not num:
            if start:
                results = results[start:]
        else:
            end = start + num
            results = results[start:end]

        # if more than one GET then flatten if groups not wanted
        if get and len(get) > 1:
            if not groups:
                results = list(chain(*results))

        # either store value and return length of results or just return results
        if store:
            self.redis[self._encode(store)] = results
            return len(results)
        else:
            return results

    # SCAN COMMANDS #

    def _common_scan(self, values_function, cursor='0', match=None, count=10, key=None):
        """
        Common scanning skeleton.

        :param key: optional function used to identify what 'match' is applied to
        """
        if count is None:
            count = 10
        cursor = int(cursor)
        count = int(count)
        if not count:
            raise ValueError('if specified, count must be > 0: %s' % count)

        values = values_function()
        if cursor + count >= len(values):
            # we reached the end, back to zero
            result_cursor = 0
        else:
            result_cursor = cursor + count

        values = values[cursor:cursor+count]

        if match is not None:
            regex = re.compile(b'^' + re.escape(self._encode(match)).replace(b'\\*', b'.*') + b'$')
            if not key:
                key = lambda v: v
            values = [v for v in values if regex.match(key(v))]

        return [result_cursor, values]

    def scan(self, cursor='0', match=None, count=10):
        """Emulate scan."""
        def value_function():
            return sorted(self.redis.keys())  # sorted list for consistent order
        return self._common_scan(value_function, cursor=cursor, match=match, count=count)

    def scan_iter(self, match=None, count=10):
        """Emulate scan_iter."""
        cursor = '0'
        while cursor != 0:
            cursor, data = self.scan(cursor=cursor, match=match, count=count)
            for item in data:
                yield item

    def sscan(self, name, cursor='0', match=None, count=10):
        """Emulate sscan."""
        def value_function():
            members = list(self.smembers(name))
            members.sort()  # sort for consistent order
            return members
        return self._common_scan(value_function, cursor=cursor, match=match, count=count)

    def sscan_iter(self, name, match=None, count=10):
        """Emulate sscan_iter."""
        cursor = '0'
        while cursor != 0:
            cursor, data = self.sscan(name, cursor=cursor,
                                      match=match, count=count)
            for item in data:
                yield item

    def zscan(self, name, cursor='0', match=None, count=10):
        """Emulate zscan."""
        def value_function():
            values = self.zrange(name, 0, -1, withscores=True)
            values.sort(key=lambda x: x[1])  # sort for consistent order
            return values
        return self._common_scan(value_function, cursor=cursor, match=match, count=count, key=lambda v: v[0])  # noqa

    def zscan_iter(self, name, match=None, count=10):
        """Emulate zscan_iter."""
        cursor = '0'
        while cursor != 0:
            cursor, data = self.zscan(name, cursor=cursor, match=match,
                                      count=count)
            for item in data:
                yield item

    def hscan(self, name, cursor='0', match=None, count=10):
        """Emulate hscan."""
        def value_function():
            values = self.hgetall(name)
            values = list(values.items())  # list of tuples for sorting and matching
            values.sort(key=lambda x: x[0])  # sort for consistent order
            return values
        scanned = self._common_scan(value_function, cursor=cursor, match=match, count=count, key=lambda v: v[0])  # noqa
        scanned[1] = dict(scanned[1])  # from list of tuples back to dict
        return scanned

    def hscan_iter(self, name, match=None, count=10):
        """Emulate hscan_iter."""
        cursor = '0'
        while cursor != 0:
            cursor, data = self.hscan(name, cursor=cursor,
                                      match=match, count=count)
            for item in data.items():
                yield item

    # SET COMMANDS #

    def sadd(self, key, *values):
        """Emulate sadd."""
        if len(values) == 0:
            raise ResponseError("wrong number of arguments for 'sadd' command")
        redis_set = self._get_set(key, 'SADD', create=True)
        before_count = len(redis_set)
        redis_set.update(map(self._encode, values))
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
        self.redis[self._encode(dest)] = result
        return len(result)

    def sinter(self, keys, *args):
        """Emulate sinter."""
        func = lambda left, right: left.intersection(right)
        return self._apply_to_sets(func, "SINTER", keys, *args)

    def sinterstore(self, dest, keys, *args):
        """Emulate sinterstore."""
        result = self.sinter(keys, *args)
        self.redis[self._encode(dest)] = result
        return len(result)

    def sismember(self, name, value):
        """Emulate sismember."""
        redis_set = self._get_set(name, 'SISMEMBER')
        if not redis_set:
            return 0

        result = self._encode(value) in redis_set
        return 1 if result else 0

    def smembers(self, name):
        """Emulate smembers."""
        return self._get_set(name, 'SMEMBERS').copy()

    def smove(self, src, dst, value):
        """Emulate smove."""
        src_set = self._get_set(src, 'SMOVE')
        dst_set = self._get_set(dst, 'SMOVE')
        value = self._encode(value)

        if value not in src_set:
            return False

        src_set.discard(value)
        dst_set.add(value)
        self.redis[self._encode(src)], self.redis[self._encode(dst)] = src_set, dst_set
        return True

    def spop(self, name):
        """Emulate spop."""
        redis_set = self._get_set(name, 'SPOP')
        if not redis_set:
            return None
        member = choice(list(redis_set))
        redis_set.remove(member)
        if len(redis_set) == 0:
            self.delete(name)
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
            redis_set.discard(self._encode(value))
        after_count = len(redis_set)
        if before_count > 0 and len(redis_set) == 0:
            self.delete(key)
        return before_count - after_count

    def sunion(self, keys, *args):
        """Emulate sunion."""
        func = lambda left, right: left.union(right)
        return self._apply_to_sets(func, "SUNION", keys, *args)

    def sunionstore(self, dest, keys, *args):
        """Emulate sunionstore."""
        result = self.sunion(keys, *args)
        self.redis[self._encode(dest)] = result
        return len(result)

    # SORTED SET COMMANDS #

    def zadd(self, name, *args, **kwargs):
        zset = self._get_zset(name, "ZADD", create=True)

        pieces = []

        # args
        if len(args) % 2 != 0:
            raise RedisError("ZADD requires an equal number of "
                             "values and scores")
        for i in xrange(len(args) // 2):
            # interpretation of args order depends on whether Redis
            # or StrictRedis is used
            score = args[2 * i + (0 if self.strict else 1)]
            member = args[2 * i + (1 if self.strict else 0)]
            pieces.append((member, score))

        # kwargs
        pieces.extend(kwargs.items())

        insert_count = lambda member, score: 1 if zset.insert(self._encode(member), float(score)) else 0  # noqa
        return sum((insert_count(member, score) for member, score in pieces))

    def zcard(self, name):
        zset = self._get_zset(name, "ZCARD")

        return len(zset) if zset is not None else 0

    def zcount(self, name, min, max):
        zset = self._get_zset(name, "ZCOUNT")

        if not zset:
            return 0

        return len(zset.scorerange(float(min), float(max)))

    def zincrby(self, name, value, amount=1):
        zset = self._get_zset(name, "ZINCRBY", create=True)

        value = self._encode(value)
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
        for member, scores in members.items():
            if len(scores) != len(keys):
                continue
            intersection[member] = reduce(aggregate_func, scores)

        # always override existing keys
        self.redis[self._encode(dest)] = intersection
        return len(intersection)

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float):
        zset = self._get_zset(name, "ZRANGE")

        if not zset:
            return []

        start, end = self._translate_range(len(zset), start, end)

        func = self._range_func(withscores, score_cast_func)
        return [func(item) for item in zset.range(start, end, desc)]

    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float):
        if (start is None) ^ (num is None):
            raise RedisError('`start` and `num` must both be specified')

        zset = self._get_zset(name, "ZRANGEBYSCORE")

        if not zset:
            return []

        func = self._range_func(withscores, score_cast_func)
        include_start, min = self._score_inclusive(min)
        include_end, max = self._score_inclusive(max)
        scorerange = zset.scorerange(min, max, start_inclusive=include_start, end_inclusive=include_end)  # noqa
        if start is not None and num is not None:
            start, num = self._translate_limit(len(scorerange), int(start), int(num))
            scorerange = scorerange[start:start + num]
        return [func(item) for item in scorerange]

    def zrank(self, name, value):
        zset = self._get_zset(name, "ZRANK")

        return zset.rank(self._encode(value)) if zset else None

    def zrem(self, name, *values):
        zset = self._get_zset(name, "ZREM")

        if not zset:
            return 0

        count_removals = lambda value: 1 if zset.remove(self._encode(value)) else 0
        removal_count = sum((count_removals(value) for value in values))
        if removal_count > 0 and len(zset) == 0:
            self.delete(name)
        return removal_count

    def zremrangebyrank(self, name, start, end):
        zset = self._get_zset(name, "ZREMRANGEBYRANK")

        if not zset:
            return 0

        start, end = self._translate_range(len(zset), start, end)
        count_removals = lambda score, member: 1 if zset.remove(member) else 0
        removal_count = sum((count_removals(score, member) for score, member in zset.range(start, end)))  # noqa
        if removal_count > 0 and len(zset) == 0:
            self.delete(name)
        return removal_count

    def zremrangebyscore(self, name, min, max):
        zset = self._get_zset(name, "ZREMRANGEBYSCORE")

        if not zset:
            return 0

        count_removals = lambda score, member: 1 if zset.remove(member) else 0
        include_start, min = self._score_inclusive(min)
        include_end, max = self._score_inclusive(max)

        removal_count = sum((count_removals(score, member)
                             for score, member in zset.scorerange(min, max,
                                                                  start_inclusive=include_start,
                                                                  end_inclusive=include_end)))
        if removal_count > 0 and len(zset) == 0:
            self.delete(name)
        return removal_count

    def zrevrange(self, name, start, end, withscores=False,
                  score_cast_func=float):
        return self.zrange(name, start, end,
                           desc=True, withscores=withscores, score_cast_func=score_cast_func)

    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float):

        if (start is None) ^ (num is None):
            raise RedisError('`start` and `num` must both be specified')

        zset = self._get_zset(name, "ZREVRANGEBYSCORE")
        if not zset:
            return []

        func = self._range_func(withscores, score_cast_func)
        include_start, min = self._score_inclusive(min)
        include_end, max = self._score_inclusive(max)

        scorerange = [x for x in reversed(zset.scorerange(float(min), float(max),
                                                          start_inclusive=include_start,
                                                          end_inclusive=include_end))]
        if start is not None and num is not None:
            start, num = self._translate_limit(len(scorerange), int(start), int(num))
            scorerange = scorerange[start:start + num]
        return [func(item) for item in scorerange]

    def zrevrank(self, name, value):
        zset = self._get_zset(name, "ZREVRANK")

        if zset is None:
            return None

        rank = zset.rank(self._encode(value))
        if rank is None:
            return None

        return len(zset) - rank - 1

    def zscore(self, name, value):
        zset = self._get_zset(name, "ZSCORE")

        return zset.score(self._encode(value)) if zset is not None else None

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
        self.redis[self._encode(dest)] = union
        return len(union)

    # Script Commands #

    def eval(self, script, numkeys, *keys_and_args):
        """Emulate eval"""
        sha = self.script_load(script)
        return self.evalsha(sha, numkeys, *keys_and_args)

    def evalsha(self, sha, numkeys, *keys_and_args):
        """Emulates evalsha"""
        if not self.script_exists(sha)[0]:
            raise RedisError("Sha not registered")
        script_callable = Script(self, self.shas[sha], self.load_lua_dependencies)
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
        sha_digest = sha1(script.encode("utf-8")).hexdigest()
        self.shas[sha_digest] = script
        return sha_digest

    def register_script(self, script):
        """Emulate register_script"""
        return Script(self, script, self.load_lua_dependencies)

    def call(self, command, *args):
        """
        Sends call to the function, whose name is specified by command.

        Used by Script invocations and normalizes calls using standard
        Redis arguments to use the expected redis-py arguments.
        """
        command = self._normalize_command_name(command)
        args = self._normalize_command_args(command, *args)

        redis_function = getattr(self, command)
        value = redis_function(*args)
        return self._normalize_command_response(command, value)

    def _normalize_command_name(self, command):
        """
        Modifies the command string to match the redis client method name.
        """
        command = command.lower()

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

        if command in ('zrangebyscore', 'zrevrangebyscore'):
            # expected format is: <command> name min max start num with_scores score_cast_func
            if len(args) <= 3:
                # just plain min/max
                return args

            start, num = None, None
            withscores = False

            for i, arg in enumerate(args[3:], 3):
                # keywords are case-insensitive
                lower_arg = self._encode(arg).lower()

                # handle "limit"
                if lower_arg == b"limit" and i + 2 < len(args):
                    start, num = args[i + 1], args[i + 2]

                # handle "withscores"
                if lower_arg == b"withscores":
                    withscores = True

            # do not expect to set score_cast_func

            return args[:3] + (start, num, withscores)

        return args

    def _normalize_command_response(self, command, response):
        if command in ('zrange', 'zrevrange', 'zrangebyscore', 'zrevrangebyscore'):
            if response and isinstance(response[0], tuple):
                return [value for tpl in response for value in tpl]

        return response

    # Config Set/Get commands #

    def config_set(self, name, value):
        """
        Set a configuration parameter.
        """
        self.redis_config[name] = value

    def config_get(self, pattern='*'):
        """
        Get one or more configuration parameters.
        """
        result = {}
        for name, value in self.redis_config.items():
            if fnmatch.fnmatch(name, pattern):
                try:
                    result[name] = int(value)
                except ValueError:
                    result[name] = value
        return result

    # PubSub commands #

    def publish(self, channel, message):
        self.pubsub[channel].append(message)

    # Internal #

    def _get_list(self, key, operation, create=False):
        """
        Get (and maybe create) a list by name.
        """
        return self._get_by_type(key, operation, create, b'list', [])

    def _get_set(self, key, operation, create=False):
        """
        Get (and maybe create) a set by name.
        """
        return self._get_by_type(key, operation, create, b'set', set())

    def _get_hash(self, name, operation, create=False):
        """
        Get (and maybe create) a hash by name.
        """
        return self._get_by_type(name, operation, create, b'hash', {})

    def _get_zset(self, name, operation, create=False):
        """
        Get (and maybe create) a sorted set by name.
        """
        return self._get_by_type(name, operation, create, b'zset', SortedSet(), return_default=False)  # noqa

    def _get_by_type(self, key, operation, create, type_, default, return_default=True):
        """
        Get (and maybe create) a redis data structure by name and type.
        """
        key = self._encode(key)
        if self.type(key) in [type_, b'none']:
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
            return lambda score_member: (score_member[1], score_cast_func(self._encode(score_member[0])))  # noqa
        else:
            return lambda score_member: score_member[1]

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
            raise TypeError("{} takes at least two arguments".format(operation.lower()))
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

    def _score_inclusive(self, score):
        if isinstance(score, basestring) and score[0] == '(':
            return False, float(score[1:])
        return True, float(score)

    def _encode(self, value):
        "Return a bytestring representation of the value. Taken from redis-py connection.py"
        if isinstance(value, bytes):
            return value
        elif isinstance(value, (int, long)):
            value = str(value).encode('utf-8')
        elif isinstance(value, float):
            value = repr(value).encode('utf-8')
        elif not isinstance(value, basestring):
            value = str(value).encode('utf-8')
        else:
            value = value.encode('utf-8', 'strict')
        return value


def get_total_milliseconds(td):
    return int((td.days * 24 * 60 * 60 + td.seconds) * 1000 + td.microseconds / 1000.0)


def mock_redis_client(**kwargs):
    """
    Mock common.util.redis_client so we
    can return a MockRedis object
    instead of a Redis object.
    """
    return MockRedis()

mock_redis_client.from_url = mock_redis_client


def mock_strict_redis_client(**kwargs):
    """
    Mock common.util.redis_client so we
    can return a MockRedis object
    instead of a StrictRedis object.
    """
    return MockRedis(strict=True)

mock_strict_redis_client.from_url = mock_strict_redis_client
