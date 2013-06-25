Version 2.7.5.1

 - Changed DEL to support a list of keys as arguments and return the number of
   keys that were deleted.
 - Improved pipeline support

Version 2.7.5.0

 - Added 'script' operations: EVAL, EVALSHA, SCRIPT_EXISTS, SCRIPT_FLUSH,
   SCRIPT_LOAD, REGISTER_SCRIPT
 - Added 'list' operations: RPOPLPUSH
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
