from hashlib import sha1

from nose.tools import eq_

from mockredis.tests.fixtures import (assert_raises_redis_error,
                                      assert_raises_watch_error,
                                      setup,
                                      teardown)


class TestPipeline(object):

    def setup(self):
        setup(self)

    def teardown(self):
        teardown(self)

    def test_pipeline(self):
        """
        Pipeline execution returns all of the saved up values.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.echo("foo")
            pipeline.echo("bar")

            eq_([b"foo", b"bar"], pipeline.execute())

    def test_pipeline_args(self):
        """
        It should be possible to pass transaction and shard_hint.
        """
        with self.redis.pipeline(transaction=False, shard_hint=None):
            pass

    def test_transaction(self):
        self.redis["a"] = 1
        self.redis["b"] = 2
        has_run = []

        def my_transaction(pipe):
            a_value = pipe.get("a")
            assert a_value in (b"1", b"2")
            b_value = pipe.get("b")
            assert b_value == b"2"

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"
            if not has_run:
                self.redis.incr("a")
                has_run.append(True)

            pipe.multi()
            pipe.set("c", int(a_value) + int(b_value))

        result = self.redis.transaction(my_transaction, "a", "b")
        eq_([True], result)
        eq_(b"4", self.redis["c"])

    def test_set_and_get(self):
        """
        Pipeline execution returns the pipeline, not the intermediate value.
        """
        with self.redis.pipeline() as pipeline:
            eq_(pipeline, pipeline.set("foo", "bar"))
            eq_(pipeline, pipeline.get("foo"))

            eq_([True, b"bar"], pipeline.execute())

    def test_scripts(self):
        """
        Verify that script calls work across pipelines.

        This test basically ensures that the pipeline shares
        state with the mock redis instance.
        """
        script_content = "redis.call('PING')"
        sha = sha1(script_content.encode("utf-8")).hexdigest()

        script_sha = self.redis.script_load(script_content)
        eq_(script_sha, sha)

        # Script exists in mock redis
        eq_([True], self.redis.script_exists(sha))

        # Script exists in pipeline
        eq_([True], self.redis.pipeline().script_exists(sha).execute()[0])

    def test_watch(self):
        """
        Verify watch puts the pipeline in immediate execution mode.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.watch("key1", "key2")
            eq_(None, pipeline.get("key1"))
            eq_(None, pipeline.get("key2"))
            eq_(True, pipeline.set("foo", "bar"))
            eq_(b"bar", pipeline.get("foo"))

    def test_multi(self):
        """
        Test explicit transaction with multi command.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.multi()
            eq_(pipeline, pipeline.set("foo", "bar"))
            eq_(pipeline, pipeline.get("foo"))

            eq_([True, b"bar"], pipeline.execute())

    def test_multi_with_watch(self):
        """
        Test explicit transaction with watched keys.
        """
        self.redis.set("foo", "bar")

        with self.redis.pipeline() as pipeline:
            pipeline.watch("foo")
            eq_(b"bar", pipeline.get("foo"))

            pipeline.multi()
            eq_(pipeline, pipeline.set("foo", "baz"))
            eq_(pipeline, pipeline.get("foo"))

            eq_([True, b"baz"], pipeline.execute())

    def test_multi_with_watch_zset(self):
        """
        Test explicit transaction with watched keys, this time with zset
        """
        self.redis.zadd("foo", "bar", 1.0)

        with self.redis.pipeline() as pipeline:
            pipeline.watch("foo")
            eq_(1, pipeline.zcard("foo"))
            pipeline.multi()
            eq_(pipeline, pipeline.zadd("foo", "baz", 2.0))
            eq_([1], pipeline.execute())

    def test_multi_with_watch_error(self):
        """
        Test explicit transaction with watched keys.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.watch("foo")
            eq_(True, pipeline.set("foo", "bar"))
            eq_(b"bar", pipeline.get("foo"))

            pipeline.multi()
            eq_(pipeline, pipeline.set("foo", "baz"))
            eq_(pipeline, pipeline.get("foo"))

            with assert_raises_watch_error():
                eq_([True, b"baz"], pipeline.execute())

    def test_watch_after_multi(self):
        """
        Cannot watch after multi.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.multi()
            with assert_raises_redis_error():
                pipeline.watch()

    def test_multiple_multi_calls(self):
        """
        Cannot call multi mutliple times.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.multi()
            with assert_raises_redis_error():
                pipeline.multi()

    def test_multi_on_implicit_transaction(self):
        """
        Cannot start an explicit transaction when commands have already been issued.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.set("foo", "bar")
            with assert_raises_redis_error():
                pipeline.multi()
