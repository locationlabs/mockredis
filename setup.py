#!/usr/bin/env python

from setuptools import setup, find_packages
import os

setup(name='mockredis',
      version='1.0' + os.environ.get('BUILD_SUFFIX', ''),
      description='Mock for redis-py',
      url='http://www.github.com/locationlabs/mockredis',
      license='Apache2',
      packages=find_packages(exclude=['*.tests']),
      namespace_packages=[
          'mockredis'
      ],
      setup_requires=[
          'nose>=1.0'
      ],
      install_requires=[
      ],
      tests_require=[
      ],
      test_suite='mockredis.tests',
      )
