#!/usr/bin/env python

from setuptools import setup, find_packages
import os

setup(name='mockredis',
      # releases for mockredis are matched
      # with the most recent redis-py version supported
      version='2.7.2' + os.environ.get('BUILD_SUFFIX', ''),
      description='Mock for redis-py',
      url='http://www.github.com/locationlabs/mockredis',
      license='Apache2',
      packages=find_packages(exclude=['*.tests']),
      setup_requires=[
          'nose>=1.0'
      ],
      install_requires=[
      ],
      tests_require=[
          'redis>=2.7.2'
      ],
      test_suite='mockredis.tests',
      )
