"""
Test redis command normalization.
"""
from nose.tools import eq_

from mockredis.client import MockRedis


def test_normalize_command_name():
    cases = [
        ("DEL", "delete"),
        ("del", "delete"),
        ("ping", "ping"),
        ("PING", "ping"),
    ]

    def _test(command, expected):
        redis = MockRedis()
        eq_(redis._normalize_command_name(command), expected)

    for command, expected in cases:
        yield _test, command, expected


def test_normalize_command_args():

    cases = [
        (False, "zadd", ("key", "member", 1.0), ("key", 1.0, "member")),
        (True, "zadd", ("key", 1.0, "member"), ("key", 1.0, "member")),

        (True, "zrevrangebyscore",
         ("key", "inf", "-inf"),
         ("key", "inf", "-inf")),

        (True, "zrevrangebyscore",
         ("key", "inf", "-inf", "limit", 0, 10),
         ("key", "inf", "-inf", 0, 10, False)),

        (True, "zrevrangebyscore",
         ("key", "inf", "-inf", "withscores"),
         ("key", "inf", "-inf", None, None, True)),

        (True, "zrevrangebyscore",
         ("key", "inf", "-inf", "withscores", "limit", 0, 10),
         ("key", "inf", "-inf", 0, 10, True)),

        (True, "zrevrangebyscore",
         ("key", "inf", "-inf", "WITHSCORES", "LIMIT", 0, 10),
         ("key", "inf", "-inf", 0, 10, True)),
    ]

    def _test(strict, command, args, expected):
        redis = MockRedis(strict=strict)
        eq_(tuple(redis._normalize_command_args(command, *args)), expected)

    for strict, command, args, expected in cases:
        yield _test, strict, command, args, expected


def test_normalize_command_response():

    cases = [
        ("get", "foo", "foo"),
        ("zrevrangebyscore", [(1, 2), (3, 4)], [1, 2, 3, 4]),
    ]

    def _test(command, response, expected):
        redis = MockRedis()
        eq_(redis._normalize_command_response(command, response), expected)

    for command, response, expected in cases:
        yield _test, command, response, expected

