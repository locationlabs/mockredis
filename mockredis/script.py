import string
import lua


class Script(object):
    """
    An executable LUA script object returned by ``MockRedis.register_script``.
    """

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        self.sha = registered_client.script_load(script)
        self._init_lua()

    def __call__(self, keys=[], args=[], client=None):
        """Execute the script, passing any required ``args``"""
        client = client or self.registered_client

        if not client.script_exists(self.sha)[0]:
            self.sha = client.scipt_load(self.script)

        return self._execute_lua(keys, args, client)

    def _init_lua(self):
        self.lg = lua.globals()
        lua.execute(lua_init_array)
        self.init_lua_array = self.lg.init_array_byname
        lua.execute(lua_callback)

    def _execute_lua(self, keys, args, client):
        """Sets KEYS and ARGV in lua globals and executes the lua redis script"""
        self.init_lua_array("KEYS", *keys)
        self.init_lua_array("ARGV", *args)
        self.lg.python_callback = Callback(client)
        return lua.execute(self.script)


class Callback(object):

    def __init__(self, client):
        self.client = client

    def __call__(self, redis_command, *redis_args):
        """
        Sends call to the function, whose name is specified by redis_command,
        in registered_client(MockRedis)
        """
        redis_function = getattr(self.client, string.lower(redis_command))
        return redis_function(*redis_args)


"""Lua script for setting an array in Lua globals"""
lua_init_array = """function init_array_byname(array_name, ...)
  _G[array_name] = arg
end
"""

"""Lua Script for sending the redis.call(...) requests from Lua to Script.callback"""
lua_callback = """redis = {
  call  = function(...)
            return python_callback(unpack(arg))
          end
}"""
