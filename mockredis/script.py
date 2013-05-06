import lua


class Script(object):
    """
    An executable LUA script object returned by ``MockRedis.register_script``.
    """

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        self.sha = registered_client.script_load(script)
        self.lg = lua.globals()

    def __call__(self, keys=[], args=[], client=None):
        """Execute the script, passing any required ``args``"""
        client = client or self.registered_client

        if not client.script_exists(self.sha)[0]:
            self.sha = client.script_load(self.script)

        return self._execute_lua(keys, args, client)

    def _execute_lua(self, keys, args, client):
        """Sets KEYS and ARGV in lua globals and executes the lua redis script"""
        self._create_lua_array("KEYS", keys)
        self._create_lua_array("ARGV", args)
        self.lg.redis = {"call": client.call}
        return lua.execute(self.script)

    def _create_lua_array(self, name, args):
        """
        Since Lua array indexes begin from 1 and we are passing a Python array to Lua.
        We need a dummy value at 0th index, which is going to be ignored in Lua scripts
        """
        self.lg[name] = [None] + list(args)
