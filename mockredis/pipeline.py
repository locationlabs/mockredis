class MockRedisPipeline(object):
    """
    Simulates a redis-python pipeline object.
    """

    def __init__(self, mock_redis):
        self.mock_redis = mock_redis
        self.commands = []

    def __getattr__(self, name):
        """
        Handle all unfound attributes by adding a deferred function call that
        delegates to the underlying mock redis instance.
        """
        command = getattr(self.mock_redis, name)
        if not callable(command):
            raise AttributeError(name)

        def wrapper(*args, **kwargs):
            self.commands.append(lambda: command(*args, **kwargs))
            return self
        return wrapper

    def execute(self):
        """
        Execute all of the saved commands and return results.
        """
        return [command() for command in self.commands]

    def __exit__(self, *argv, **kwargs):
        pass

    def __enter__(self, *argv, **kwargs):
        return self
