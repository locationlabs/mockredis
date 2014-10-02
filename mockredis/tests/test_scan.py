from nose.tools import eq_

from mockredis.tests.fixtures import setup


class TestRedisEmptyScans(object):
    """zero scan results tests"""

    def setup(self):
        setup(self)

    def test_scans(self):
        def eq_scan(results, cursor, elements):
            """
            Explicitly compare cursor and element by index as there
            redis-py currently returns a tuple for HSCAN and a list
            for the others, mockredis-py only returns lists, and it's
            not clear that emulating redis-py in this regard is "correct".
            """
            eq_(results[0], cursor)
            eq_(results[1], elements)

        eq_scan(self.redis.scan(), '0', [])
        eq_scan(self.redis.sscan("foo"), '0', [])
        eq_scan(self.redis.zscan("foo"), '0', [])
        eq_scan(self.redis.hscan("foo"), '0', {})


class TestRedisScan(object):
    """SCAN tests"""

    def setup(self):
        setup(self)
        self.redis.set('key_abc_1', '1')
        self.redis.set('key_abc_2', '2')
        self.redis.set('key_abc_3', '3')
        self.redis.set('key_abc_4', '4')
        self.redis.set('key_abc_5', '5')
        self.redis.set('key_abc_6', '6')

        self.redis.set('key_xyz_1', '1')
        self.redis.set('key_xyz_2', '2')
        self.redis.set('key_xyz_3', '3')
        self.redis.set('key_xyz_4', '4')
        self.redis.set('key_xyz_5', '5')

    def test_scan(self):
        def do_full_scan(match, count):
            keys = set()  # technically redis SCAN can return duplicate keys
            cursor = '0'
            result_cursor = None
            while result_cursor != '0':
                results = self.redis.scan(cursor=cursor, match=match, count=count)
                keys.update(results[1])
                cursor = results[0]
                result_cursor = cursor
            return keys

        abc_keys = set([b'key_abc_1', b'key_abc_2', b'key_abc_3', b'key_abc_4', b'key_abc_5', b'key_abc_6'])
        eq_(do_full_scan('*abc*', 1), abc_keys)
        eq_(do_full_scan('*abc*', 2), abc_keys)
        eq_(do_full_scan('*abc*', 10), abc_keys)

        xyz_keys = set([b'key_xyz_1', b'key_xyz_2', b'key_xyz_3', b'key_xyz_4', b'key_xyz_5'])
        eq_(do_full_scan('*xyz*', 1), xyz_keys)
        eq_(do_full_scan('*xyz*', 2), xyz_keys)
        eq_(do_full_scan('*xyz*', 10), xyz_keys)

        one_keys = set([b'key_abc_1', b'key_xyz_1'])
        eq_(do_full_scan('*_1', 1), one_keys)
        eq_(do_full_scan('*_1', 2), one_keys)
        eq_(do_full_scan('*_1', 10), one_keys)

        all_keys = abc_keys.union(xyz_keys)
        eq_(do_full_scan('*', 1), all_keys)
        eq_(do_full_scan('*', 2), all_keys)
        eq_(do_full_scan('*', 10), all_keys)


class TestRedisSScan(object):
    """SSCAN tests"""

    def setup(self):
        setup(self)
        self.redis.sadd('key', 'abc_1')
        self.redis.sadd('key', 'abc_2')
        self.redis.sadd('key', 'abc_3')
        self.redis.sadd('key', 'abc_4')
        self.redis.sadd('key', 'abc_5')
        self.redis.sadd('key', 'abc_6')

        self.redis.sadd('key', 'xyz_1')
        self.redis.sadd('key', 'xyz_2')
        self.redis.sadd('key', 'xyz_3')
        self.redis.sadd('key', 'xyz_4')
        self.redis.sadd('key', 'xyz_5')

    def test_scan(self):
        def do_full_scan(name, match, count):
            keys = set()  # technically redis SCAN can return duplicate keys
            cursor = '0'
            result_cursor = None
            while result_cursor != '0':
                results = self.redis.sscan(name, cursor=cursor, match=match, count=count)
                keys.update(results[1])
                cursor = results[0]
                result_cursor = cursor
            return keys

        abc_members = set([b'abc_1', b'abc_2', b'abc_3', b'abc_4', b'abc_5', b'abc_6'])
        eq_(do_full_scan('key', '*abc*', 1), abc_members)
        eq_(do_full_scan('key', '*abc*', 2), abc_members)
        eq_(do_full_scan('key', '*abc*', 10), abc_members)

        xyz_members = set([b'xyz_1', b'xyz_2', b'xyz_3', b'xyz_4', b'xyz_5'])
        eq_(do_full_scan('key', '*xyz*', 1), xyz_members)
        eq_(do_full_scan('key', '*xyz*', 2), xyz_members)
        eq_(do_full_scan('key', '*xyz*', 10), xyz_members)

        one_members = set([b'abc_1', b'xyz_1'])
        eq_(do_full_scan('key', '*_1', 1), one_members)
        eq_(do_full_scan('key', '*_1', 2), one_members)
        eq_(do_full_scan('key', '*_1', 10), one_members)

        all_members = abc_members.union(xyz_members)
        eq_(do_full_scan('key', '*', 1), all_members)
        eq_(do_full_scan('key', '*', 2), all_members)
        eq_(do_full_scan('key', '*', 10), all_members)


class TestRedisZScan(object):
    """ZSCAN tests"""

    def setup(self):
        setup(self)
        self.redis.zadd('key', 'abc_1', 1)
        self.redis.zadd('key', 'abc_2', 2)
        self.redis.zadd('key', 'abc_3', 3)
        self.redis.zadd('key', 'abc_4', 4)
        self.redis.zadd('key', 'abc_5', 5)
        self.redis.zadd('key', 'abc_6', 6)

        self.redis.zadd('key', 'xyz_1', 1)
        self.redis.zadd('key', 'xyz_2', 2)
        self.redis.zadd('key', 'xyz_3', 3)
        self.redis.zadd('key', 'xyz_4', 4)
        self.redis.zadd('key', 'xyz_5', 5)

    def test_scan(self):
        def do_full_scan(name, match, count):
            keys = set()  # technically redis SCAN can return duplicate keys
            cursor = '0'
            result_cursor = None
            while result_cursor != '0':
                results = self.redis.zscan(name, cursor=cursor, match=match, count=count)
                keys.update(results[1])
                cursor = results[0]
                result_cursor = cursor
            return keys

        abc_members = set([(b'abc_1', 1), (b'abc_2', 2), (b'abc_3', 3), (b'abc_4', 4), (b'abc_5', 5), (b'abc_6', 6)])
        eq_(do_full_scan('key', '*abc*', 1), abc_members)
        eq_(do_full_scan('key', '*abc*', 2), abc_members)
        eq_(do_full_scan('key', '*abc*', 10), abc_members)

        xyz_members = set([(b'xyz_1', 1), (b'xyz_2', 2), (b'xyz_3', 3), (b'xyz_4', 4), (b'xyz_5', 5)])
        eq_(do_full_scan('key', '*xyz*', 1), xyz_members)
        eq_(do_full_scan('key', '*xyz*', 2), xyz_members)
        eq_(do_full_scan('key', '*xyz*', 10), xyz_members)

        one_members = set([(b'abc_1', 1), (b'xyz_1', 1)])
        eq_(do_full_scan('key', '*_1', 1), one_members)
        eq_(do_full_scan('key', '*_1', 2), one_members)
        eq_(do_full_scan('key', '*_1', 10), one_members)

        all_members = abc_members.union(xyz_members)
        eq_(do_full_scan('key', '*', 1), all_members)
        eq_(do_full_scan('key', '*', 2), all_members)
        eq_(do_full_scan('key', '*', 10), all_members)


class TestRedisHScan(object):
    """HSCAN tests"""

    def setup(self):
        setup(self)
        self.redis.hset('key', 'abc_1', 1)
        self.redis.hset('key', 'abc_2', 2)
        self.redis.hset('key', 'abc_3', 3)
        self.redis.hset('key', 'abc_4', 4)
        self.redis.hset('key', 'abc_5', 5)
        self.redis.hset('key', 'abc_6', 6)

        self.redis.hset('key', 'xyz_1', 1)
        self.redis.hset('key', 'xyz_2', 2)
        self.redis.hset('key', 'xyz_3', 3)
        self.redis.hset('key', 'xyz_4', 4)
        self.redis.hset('key', 'xyz_5', 5)

    def test_scan(self):
        def do_full_scan(name, match, count):
            keys = {}
            cursor = '0'
            result_cursor = None
            while result_cursor != '0':
                results = self.redis.hscan(name, cursor=cursor, match=match, count=count)
                keys.update(results[1])
                cursor = results[0]
                result_cursor = cursor
            return keys

        abc = {b'abc_1': b'1', b'abc_2': b'2', b'abc_3': b'3', b'abc_4': b'4', b'abc_5': b'5', b'abc_6': b'6'}
        eq_(do_full_scan('key', '*abc*', 1), abc)
        eq_(do_full_scan('key', '*abc*', 2), abc)
        eq_(do_full_scan('key', '*abc*', 10), abc)

        xyz = {b'xyz_1': b'1', b'xyz_2': b'2', b'xyz_3': b'3', b'xyz_4': b'4', b'xyz_5': b'5'}
        eq_(do_full_scan('key', '*xyz*', 1), xyz)
        eq_(do_full_scan('key', '*xyz*', 2), xyz)
        eq_(do_full_scan('key', '*xyz*', 10), xyz)

        all_1 = {b'abc_1': b'1', b'xyz_1': b'1'}
        eq_(do_full_scan('key', '*_1', 1), all_1)
        eq_(do_full_scan('key', '*_1', 2), all_1)
        eq_(do_full_scan('key', '*_1', 10), all_1)

        abcxyz = abc
        abcxyz.update(xyz)
        eq_(do_full_scan('key', '*', 1), abcxyz)
        eq_(do_full_scan('key', '*', 2), abcxyz)
        eq_(do_full_scan('key', '*', 10), abcxyz)
