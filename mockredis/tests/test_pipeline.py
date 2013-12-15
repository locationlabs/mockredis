from hashlib import sha1

from nose.tools import assert_raises, eq_

from mockredis import MockRedis
from mockredis.exceptions import RedisError


class TestPipeline(object):

    def setup(self):
        self.redis = MockRedis()
        self.redis.flushdb()

    def test_pipeline(self):
        """
        Pipeline execution returns all of the saved up values.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.echo("foo")
            pipeline.echo("bar")

            eq_(["foo", "bar"], pipeline.execute())

    def test_pipeline_args(self):
        """
        It should be possible to pass transaction and shard_hint.
        """
        with self.redis.pipeline(transaction=False, shard_hint=None):
            pass

    def test_set_and_get(self):
        """
        Pipeline execution returns the pipeline, not the intermediate value.
        """
        with self.redis.pipeline() as pipeline:
            eq_(pipeline, pipeline.set("foo", "bar"))
            eq_(pipeline, pipeline.get("foo"))

            eq_([True, "bar"], pipeline.execute())

    def test_scripts(self):
        """
        Verify that script calls work across pipelines.

        This test basically ensures that the pipeline shares
        state with the mock redis instance.
        """
        script_content = "redis.call('PING')"
        sha = sha1(script_content).hexdigest()

        self.redis.register_script(script_content)

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
            eq_(True, pipeline.set("foo", "bar"))
            eq_("bar", pipeline.get("foo"))

    def test_multi(self):
        """
        Test explicit transaction with multi command.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.multi()
            eq_(pipeline, pipeline.set("foo", "bar"))
            eq_(pipeline, pipeline.get("foo"))

            eq_([True, "bar"], pipeline.execute())

    def test_multi_with_watch(self):
        """
        Test explicit transaction with watched keys.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.watch("foo")
            eq_(True, pipeline.set("foo", "bar"))
            eq_("bar", pipeline.get("foo"))

            pipeline.multi()
            eq_(pipeline, pipeline.set("foo", "baz"))
            eq_(pipeline, pipeline.get("foo"))

            eq_([True, "baz"], pipeline.execute())

    def test_watch_after_multi(self):
        """
        Cannot watch after multi.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.multi()
            with assert_raises(RedisError):
                pipeline.watch()

    def test_multiple_multi_calls(self):
        """
        Cannot call multi mutliple times.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.multi()
            with assert_raises(RedisError):
                pipeline.multi()

    def test_multi_on_implicit_transaction(self):
        """
        Cannot start an explicit transaction when commands have already been issued.
        """
        with self.redis.pipeline() as pipeline:
            pipeline.set("foo", "bar")
            with assert_raises(RedisError):
                pipeline.multi()
