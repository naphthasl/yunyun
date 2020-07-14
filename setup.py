#!/usr/bin/env python

import sys
from setuptools import setup

if sys.version_info < (2, 7):
    raise NotImplementedError("Sorry, Python 2.X isn't supported.")

import yunyun

setup(name='yunyundb',
    version=yunyun.__version__,
    description='Yunyun is intended to be a simplified persistent data storage system.',
    long_description=yunyun.__doc__,
    long_description_content_type="text/markdown",
    author=yunyun.__author__,
    author_email='naphtha@lotte.link',
    url='https://github.com/naphthasl/yunyun',
    py_modules=['yunyun'],
    license=yunyun.__license__,
    install_requires=[
        'xxhash',
        'filelock',
        'lz4'
    ],
    platforms='any',
    python_requires='>=3.6',
    classifiers=[
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
        ],
    )
