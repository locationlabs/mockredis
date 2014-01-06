from copy import deepcopy

from mockredis.exceptions import RedisError, WatchError


class MockRedisPipeline(object):
    """
    Simulates a redis-python pipeline object.
    """

    def __init__(self, mock_redis, transaction=True, shard_hint=None):
        self.mock_redis = mock_redis
        self._reset()

    def __getattr__(self, name):
        """
        Handle all unfound attributes by adding a deferred function call that
        delegates to the underlying mock redis instance.
        """
        command = getattr(self.mock_redis, name)
        if not callable(command):
            raise AttributeError(name)

        def wrapper(*args, **kwargs):
            if self.watching and not self.explicit_transaction:
                # execute the command immediately
                return command(*args, **kwargs)
            else:
                self.commands.append(lambda: command(*args, **kwargs))
                return self
        return wrapper

    def watch(self, *keys):
        """
        Put the pipeline into immediate execution mode.
        Does not actually watch any keys.
        """
        if self.explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")
        self.watching = True
        for key in keys:
            self._watched_keys[key] = deepcopy(self.mock_redis.redis.get(key))

    def multi(self):
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        if self.explicit_transaction:
            raise RedisError("Cannot issue nested calls to MULTI")
        if self.commands:
            raise RedisError("Commands without an initial WATCH have already been issued")
        self.explicit_transaction = True

    def execute(self):
        """
        Execute all of the saved commands and return results.
        """
        try:
            for key, value in self._watched_keys.items():
                if self.mock_redis.redis.get(key) != value:
                    raise WatchError("Watched variable changed.")
            return [command() for command in self.commands]
        finally:
            self._reset()

    def _reset(self):
        """
        Reset instance variables.
        """
        self.commands = []
        self.watching = False
        self._watched_keys = {}
        self.explicit_transaction = False

    def __exit__(self, *argv, **kwargs):
        pass

    def __enter__(self, *argv, **kwargs):
        return self
