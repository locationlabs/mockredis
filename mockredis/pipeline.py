class MockRedisPipeline(object):
    """
    Simulates a redis-python pipeline object.

    Simulation is currently a noop; all redis operations
    are run immediately and execute() does nothing. This logic
    could be improved quite a bit.
    """

    def __init__(self, mock_redis):
        self.mock_redis = mock_redis
        self.results = []

    def __getattr__(self, name):
        """
        Delegate any unfound attributes to underlying mock redis instance.
        """
        return getattr(self.mock_redis, name)

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
