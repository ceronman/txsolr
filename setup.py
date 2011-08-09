#!/usr/bin/env python
# -*- coding: utf-8 -*-
from distutils.core import setup

setup(
    name='txsolr',
    version='0.1.0',
    description=('Twisted-based asynchronous client library for Solr'
                 'Enterprise Search Server'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Framework :: Twisted',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.6',
        'Topic :: Software Development :: Libraries',
        'Topic :: Text Processing :: Indexing'],
    author='Manuel Cerón',
    author_email='manuel@fluidinfo.com',
    url='https://launchpad.net/txsolr',
    packages=['txsolr'])
