# Mock for the redis-py client library

Supports writing tests for code using the [redis-py][redis-py] library 
without requiring a a [redis-server][redis] install.

## Usage

`mockredis.make_redis_client` can be used to patch instances of the *redis client*. 

## Attribution

This code is shamelessly derived from work by [John DeRosa][john].

[redis-py]: https://github.com/andymccurdy/redis-py
[redis]:    http://redis.io
[john]:     http://seeknuance.com/2012/02/18/replacing-redis-with-a-python-mock/

