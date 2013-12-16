"""
Simple clock abstraction.
"""
from abc import ABCMeta, abstractmethod
from datetime import datetime


class Clock(object):
    """
    A clock knows the current time.

    Clock can be subclassed for testing scenarios that need to control for time.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def now(self):
        pass


class SystemClock(Clock):

    def now(self):
        return datetime.now()
