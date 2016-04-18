"""
Test redis client factory functions.
"""
from nose.tools import ok_

from mockredis import mock_redis_client, mock_strict_redis_client


def test_mock_redis_client():
    """
    Test that we can pass kwargs to the Redis mock/patch target.
    """
    ok_(not mock_redis_client(host="localhost", port=6379).strict)


def test_mock_redis_client_from_url():
    """
    Test that we can pass kwargs to the Redis from_url mock/patch target.
    """
    ok_(not mock_redis_client.from_url(host="localhost", port=6379).strict)


def test_mock_strict_redis_client():
    """
    Test that we can pass kwargs to the StrictRedis mock/patch target.
    """
    ok_(mock_strict_redis_client(host="localhost", port=6379).strict)


def test_mock_strict_redis_client_from_url():
    """
    Test that we can pass kwargs to the StrictRedis from_url mock/patch target.
    """
    ok_(mock_strict_redis_client.from_url(host="localhost", port=6379).strict)
