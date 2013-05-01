import string
import lua


class Script(object):
    """
    An executable LUA script object returned by ``MockRedis.register_script``.
    Also acts as sha for ''MockRedis.shas''
    """

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        lua.execute(lua_callback)
        self.lg = lua.globals()

    def __call__(self, keys=[], args=[], client=None):
        "Execute the script, passing any required ``args``"

        # make sure the Redis server knows about the script
        if client:
            args = tuple(keys) + tuple(args)
            return client.eval(self.script, len(keys), *args)

        if not self.registered_client.script_exists_single(self.script):
            self.registered_client.register_sha(self)

        return self._execute_lua(keys, args)

    def _execute_lua(self, keys, args):
        """Sets KEYS and ARGV in lua globals and executes the lua redis script"""
        self._create_lua_array("KEYS", keys)
        self._create_lua_array("ARGV", args)
        self.lg.python_callback = Callback(self.registered_client)
        return lua.execute(self.script)

    def _create_lua_array(self, name, args):
        """
        Since Lua array indexes begin from 1 and we are passing a Python array to Lua.
        We need a dummy value at 0th index, which is going to be ignored in Lua scripts
        """
        self.lg[name] = [None] + list(args)


class Callback(object):

    def __init__(self, registered_client):
        self.registered_client = registered_client

    def __call__(self, redis_command, *redis_args):
        """
        Sends call to the function, whose name is specified by redis_command,
        in registered_client(MockRedis)
        """
        redis_function = getattr(self.registered_client, string.lower(redis_command))
        return redis_function(*redis_args)

"""Lua Script for sending the redis.call(...) requests from Lua to Script.callback"""
lua_callback = """redis = {
  call  = function(...)
            return python_callback(unpack(arg))
          end
}"""
