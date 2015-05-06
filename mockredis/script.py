import sys
import threading
from mockredis.exceptions import ResponseError

LuaLock = threading.Lock()


class Script(object):
    """
    An executable Lua script object returned by ``MockRedis.register_script``.
    """

    def __init__(self, registered_client, script, load_dependencies=True):
        self.registered_client = registered_client
        self.script = script
        self.load_dependencies = load_dependencies
        self.sha = registered_client.script_load(script)

    def __call__(self, keys=[], args=[], client=None):
        """Execute the script, passing any required ``args``"""
        with LuaLock:
            client = client or self.registered_client

            if not client.script_exists(self.sha)[0]:
                self.sha = client.script_load(self.script)

            return self._execute_lua(keys, args, client)

    def _execute_lua(self, keys, args, client):
        """
        Sets KEYS and ARGV alongwith redis.call() function in lua globals
        and executes the lua redis script
        """
        lua, lua_globals = Script._import_lua(self.load_dependencies)
        lua_globals.KEYS = self._python_to_lua(keys)
        lua_globals.ARGV = self._python_to_lua(args)

        def _call(*call_args):
            # redis-py and native redis commands are mostly compatible argument
            # wise, but some exceptions need to be handled here:
            if str(call_args[0]).lower() == 'lrem':
                response = client.call(
                    call_args[0], call_args[1],
                    call_args[3],  # "count", default is 0
                    call_args[2])
            else:
                response = client.call(*call_args)
            return self._python_to_lua(response)

        lua_globals.redis = {"call": _call}
        return self._lua_to_python(lua.execute(self.script), return_status=True)

    @staticmethod
    def _import_lua(load_dependencies=True):
        """
        Import lua and dependencies.

        :param load_dependencies: should Lua library dependencies be loaded?
        :raises: RuntimeError if Lua is not available
        """
        try:
            import lua
        except ImportError:
            raise RuntimeError("Lua not installed")

        lua_globals = lua.globals()
        if load_dependencies:
            Script._import_lua_dependencies(lua, lua_globals)
        return lua, lua_globals

    @staticmethod
    def _import_lua_dependencies(lua, lua_globals):
        """
        Imports lua dependencies that are supported by redis lua scripts.

        The current implementation is fragile to the target platform and lua version
        and may be disabled if these imports are not needed.

        Included:
            - cjson lib.
        Pending:
            - base lib.
            - table lib.
            - string lib.
            - math lib.
            - debug lib.
            - cmsgpack lib.
        """
        if sys.platform not in ('darwin', 'windows'):
            import ctypes
            ctypes.CDLL('liblua5.2.so', mode=ctypes.RTLD_GLOBAL)

        try:
            lua_globals.cjson = lua.eval('require "cjson"')
        except RuntimeError:
            raise RuntimeError("cjson not installed")

    @staticmethod
    def _lua_to_python(lval, return_status=False):
        """
        Convert Lua object(s) into Python object(s), as at times Lua object(s)
        are not compatible with Python functions
        """
        import lua
        lua_globals = lua.globals()
        if lval is None:
            # Lua None --> Python None
            return None
        if lua_globals.type(lval) == "table":
            # Lua table --> Python list
            pval = []
            for i in lval:
                if return_status:
                    if i == 'ok':
                        return lval[i]
                    if i == 'err':
                        raise ResponseError(lval[i])
                pval.append(Script._lua_to_python(lval[i]))
            return pval
        elif isinstance(lval, long):
            # Lua number --> Python long
            return long(lval)
        elif isinstance(lval, float):
            # Lua number --> Python float
            return float(lval)
        elif lua_globals.type(lval) == "userdata":
            # Lua userdata --> Python string
            return str(lval)
        elif lua_globals.type(lval) == "string":
            # Lua string --> Python string
            return lval
        elif lua_globals.type(lval) == "boolean":
            # Lua boolean --> Python bool
            return bool(lval)
        raise RuntimeError("Invalid Lua type: " + str(lua_globals.type(lval)))

    @staticmethod
    def _python_to_lua(pval):
        """
        Convert Python object(s) into Lua object(s), as at times Python object(s)
        are not compatible with Lua functions
        """
        import lua
        if pval is None:
            # Python None --> Lua None
            return lua.eval("")
        if isinstance(pval, (list, tuple, set)):
            # Python list --> Lua table
            # e.g.: in lrange
            #     in Python returns: [v1, v2, v3]
            #     in Lua returns: {v1, v2, v3}
            lua_list = lua.eval("{}")
            lua_table = lua.eval("table")
            for item in pval:
                lua_table.insert(lua_list, Script._python_to_lua(item))
            return lua_list
        elif isinstance(pval, dict):
            # Python dict --> Lua dict
            # e.g.: in hgetall
            #     in Python returns: {k1:v1, k2:v2, k3:v3}
            #     in Lua returns: {k1, v1, k2, v2, k3, v3}
            lua_dict = lua.eval("{}")
            lua_table = lua.eval("table")
            for k, v in pval.iteritems():
                lua_table.insert(lua_dict, Script._python_to_lua(k))
                lua_table.insert(lua_dict, Script._python_to_lua(v))
            return lua_dict
        elif isinstance(pval, str):
            # Python string --> Lua userdata
            return pval
        elif isinstance(pval, bool):
            # Python bool--> Lua boolean
            return lua.eval(str(pval).lower())
        elif isinstance(pval, (int, long, float)):
            # Python int --> Lua number
            lua_globals = lua.globals()
            return lua_globals.tonumber(str(pval))

        raise RuntimeError("Invalid Python type: " + str(type(pval)))
