import string
import lua


class Script(object):
    "An executable LUA script object returned by ``MockRedis.register_script``"

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        self.lg = lua.globals()
        self.lg.python_callback = self.callback

    def __call__(self, keys=[], args=[]):
        "Execute the script, passing any required ``args``"
        self._execute_lua(keys, args)

    def _execute_lua(self, keys, args):
        self._create_lua_array("KEYS", keys)
        self._create_lua_array("ARGV", args)
        lua.execute(lua_callback)
        lua.execute(self.script)

    def _create_lua_array(self, name, args):
        self.lg[name] = ["dummy"] + args

    def callback(self, redis_command, *args):
        redis_function = getattr(self.registered_client, string.lower(redis_command))
        redis_args = args[1:]
        redis_function(*redis_args)
        print redis_command
        print args


lua_callback = """redis = {
  call  = function(...)
            python_callback(unpack(arg))
          end
}"""
