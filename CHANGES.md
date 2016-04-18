Version 2.9.3
 - Support for `from_url`
 - Going to remove develop and use master following github flow model.
 
Version 2.9.2
 - Fixed the versioning issue.

Version 2.9.1
 - Support for `transaction`
 - Fix `do_expire` method in Python 3  
 
Version 2.9.0.12
 - Support: `dbsize`

Version 2.9.0.11
 - Support: `scan_iter`, `sscan_iter`, `zscan_iter`, `hscan_iter`

Version 2.9.0.10
 - Return & store byte strings everywhere (unicode turns into utf-8 by default)
 - Fix *SCAN returning non-long values.
 - Fix *TTL returning -1/-2/None - this varies depending on whether StrictRedis is in use.

Version 2.9.0.9

 - Support: RENAME and RENAMENX
 - SADD will raise an exception if an empty list is passed to it

Version 2.9.0.8

 - Add inclusive syntax (parenthesis) support for zero sets ZRANGEBYSCORE, ZREVRANGEBYSCORE  & ZREMRANGEBYSCORE
 - Expire can accept a timedelta value

Version 2.9.0.1

 - Rename `redis.py` module as `client.py` to avoid naming conflicts from the nose plugin.
 - Support contextmanager uses of `MockRedisLock`
 - Support `string` operation: MSET

Version 2.9.0.0

 - Support redis-py 2.9.0
 - Support: SCAN, SSCAN, HSCAN, and ZSCAN

Version 2.8.0.3

 - Support verifying unit tests against actual redis-server and redis-py.
 - Improve exception representation/mapping.
 - Update TTL to return -2 for unknown keys.
 - Fix `zset` `score_range_func` behavior to expect string input
 - Raise `WatchError` in `MockRedisPipeline.execute()`
 - Added `list` operations: SORT

Version 2.8.0.2

 - Added `string` operations: MGET, MSETNX, and GETSET
 - Added "*" support to KEYS
 - Added container functions: __getitem__, __setitem__, __delitem__, __member__
 - Added `pubsub` operations: PUBLISH

Version 2.8.0.1

 - Fixed for RPOPLPUSH

Version 2.8.0.0

 - Update LREM argument order to match redispy

Version 2.7.5.2

 - Added `list` operations: LSET, LTRIM
 - Added `key` operations: INCRBY, DECRBY
 - Added `transaction` operations: WATCH, MULTI, UNWATCH
 - Added expiration operations: EXPIREAT, PEXPIRE, PTTL, PSETX
 - Fixed return values for some `set` operations
 
Version 2.7.5.1

 - Changed DEL to support a list of keys as arguments and return the number of
   keys that were deleted.
 - Improved pipeline support

Version 2.7.5.0

 - Added `script` operations: EVAL, EVALSHA, SCRIPT_EXISTS, SCRIPT_FLUSH,
   SCRIPT_LOAD, REGISTER_SCRIPT
 - Added `list` operations: RPOPLPUSH
 - Added `string` operations: SETEX, SETNX
 - Changed `string` operation SET to support EX, PX, NX and XX options
   (available in redis-py since 2.7.4).

Version 2.7.2.5

 - Added `hash` operations: HMGET, HSETNX, HINCRBYFLOAT, HKEYS, HVALS

Version 2.7.2.4

 - Added `list` operations: LREM

Version 2.7.2.3

 -  Changed distribution name to "mockredispy"
 -  Added `set` operations: SADD (multivalue), SCARD, SDIFF, SDIFFSTORE,
    SINTER, SINTERSTORE, SISMEMBER, SMEMBERS (minor improvement), SMOVE,
    SPOP, SRANDMEMBER (improvement), SREM (multivalue), SUNION, SUNIONSTORE

Version 2.7.2.2

 -  Added `list` operations: LLEN, LPUSH, RPOP
 -  Ensure that saved values are strings.

Version 2.7.2.1

 -  Added `zset` operations: ZADD, ZCARD, ZCOUNT, ZINCRBY, ZINTERSTORE, ZRANGE,
    ZRANGEBYSCORE, ZRANK, ZREM, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANGE,
    ZREVRANGEBYSCORE, ZREVRANK, ZSCORE, ZUNIONSTORE
