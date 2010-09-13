# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Solr Bindings for Python and Twisted
"""

__author__ = 'Manuel Cerón'
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'
__version__ = (0,1,0)

from client import SolrClient
from errors import *

##########################################################################
# Logging configuration
##########################################################################

import logging

# NOTE: this is not necessary in Python 2.7
class _NullHandler(logging.Handler):

    def emit(self, record):
        pass

_logger = logging.getLogger('txsolr')
_logger.addHandler(_NullHandler())

def logToStderr(level=logging.DEBUG):
    global _logger
    _logger.addHandler(logging.StreamHandler())
    _logger.setLevel(level)



