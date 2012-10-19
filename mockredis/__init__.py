__import__('pkg_resources').declare_namespace(__name__)


from mockredis.redis import MockRedis, mock_redis_client

__all__ = ["MockRedis", "mock_redis_client"]
