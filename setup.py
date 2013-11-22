#!/usr/bin/env python

from setuptools import setup, find_packages

# Match releases to redis-py versions
__version__ = '2.7.5.2'

# Jenkins will replace __build__ with a unique value.
__build__ = ''

setup(name='mockredispy',
      version=__version__ + __build__,
      description='Mock for redis-py',
      url='http://www.github.com/locationlabs/mockredis',
      license='Apache2',
      packages=find_packages(exclude=['*.tests']),
      setup_requires=[
          'nose'
      ],
      extras_require={
          'lua': ['lunatic-python-bugfix==1.1.1'],
      },
      tests_require=[
          'redis>=2.7.2'
      ],
      test_suite='mockredis.tests',
      )
