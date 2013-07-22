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
        lua_globals.KEYS = self._create_lua_array(keys)
        lua_globals.ARGV = self._create_lua_array(args)

        def _call(*call_args):
            """
            Convert Python list into Lua list, as Python list is not
            compatible with Lua functions such as table.getn().
            """
            response = client.call(*call_args)
            if isinstance(response, list):
                lua_list = lua.eval("{}")
                lua_table = lua.eval("table")
                for item in response:
                    lua_table.insert(lua_list, item)
                return lua_list
            return response

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

    def _create_lua_array(self, args):
        """
        Since Lua array indexes begin from 1 and we are passing a Python array to Lua.
        We need a dummy value at 0th index, which is going to be ignored in Lua scripts
        """
        return [None] + list(args)
