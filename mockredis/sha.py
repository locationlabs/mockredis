class Sha(object):

    def __init__(self, script):
        self.script = script

    def __eq__(self, other):
        return self.script == other.script
