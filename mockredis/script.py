class Script(object):
    """
    An executable LUA script object returned by ``MockRedis.register_script``.
    """

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        self.sha = registered_client.script_load(script)

    def __call__(self, keys=[], args=[], client=None):
        """Execute the script, passing any required ``args``"""
        client = client or self.registered_client

        if not client.script_exists(self.sha)[0]:
            self.sha = client.script_load(self.script)

        return self._execute_lua(keys, args, client)

    def _execute_lua(self, keys, args, client):
        """
        Sets KEYS and ARGV alongwith redis.call() function in lua globals
        and executes the lua redis script
        """
        try:
            import lua
        except ImportError:
            raise RuntimeError("LUA not installed")

        lua_globals = lua.globals()
        self._import_lua_dependencies(lua, lua_globals)
        lua_globals.KEYS = self._python_to_lua(keys)
        lua_globals.ARGV = self._python_to_lua(args)

        def _call(*call_args):
            response = client.call(*call_args)
            return self._python_to_lua(response)

        lua_globals.redis = {"call": _call}
        return lua.execute(self.script)

    def _import_lua_dependencies(self, lua, lua_globals):
        """
        Imports lua dependencies that are supported by redis lua scripts.
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

        import ctypes
        ctypes.CDLL('liblua5.1.so', mode=ctypes.RTLD_GLOBAL)

        try:
            lua_globals.cjson = lua.eval('require "cjson"')
        except RuntimeError:
            raise RuntimeError("cjson not installed")

    def _python_to_lua(self, pval):
        import lua
        if isinstance(pval, list) or isinstance(pval, tuple):
            """
            Convert Python list into Lua list, as Python list is not
            compatible with Lua functions such as table.getn().
            """
            lua_list = lua.eval("{}")
            lua_table = lua.eval("table")
            for item in pval:
                lua_table.insert(lua_list, self._python_to_lua(item))
            return lua_list
        elif isinstance(pval, dict):
            """
            Convert Python dict into Lua list, as whenever Python returns
            a dictionary; Lua returns a list with key value pairs merged
            into one single list.
            e.g.: in hmgetall
                in Python returns: {k1:v1, k2:v2, k3:v3}
                in Lua returns: {k1, v1, k2, v2, k3, v3}
            """
            lua_dict = lua.eval("{}")
            lua_table = lua.eval("table")
            for k, v in pval.iteritems():
                lua_table.insert(lua_dict, self._python_to_lua(k))
                lua_table.insert(lua_dict, self._python_to_lua(v))
            return lua_dict
        elif isinstance(pval, int):
            """
            Convert Python integer into Lua number
            """
            lua_globals = lua.globals()
            return lua_globals.tonumber(str(pval))

        return pval
