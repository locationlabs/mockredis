from hashlib import sha1
from unittest import TestCase
from nose.tools import eq_
from mockredis import MockRedis


class TestPipeline(TestCase):

    def setUp(self):
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
