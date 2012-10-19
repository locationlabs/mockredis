from mockredis.redis import MockRedis


class MockRedisPipeline(MockRedis):
    """
    Imitate a redis-python pipeline object
    so unit tests can run without needing a
    real Redis server.
    """

    def __init__(self, redis, timeouts):
        """Initialize the object."""
        self.redis = redis
        self.timeouts = timeouts

    def execute(self):
        """
        Emulate the execute method. All piped
        commands are executed immediately
        in this mock, so this is a no-op.
        """
        pass

    def __exit__(self, *argv, **kwargs):
        pass

    def __enter__(self, *argv, **kwargs):
        return self
