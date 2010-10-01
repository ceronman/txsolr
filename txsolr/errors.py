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
Errors
"""

__all__ = ['HTTPWrongStatus', 'SolrResponseError', 'HTTPRequestError',
           'InputError']

class InputError(ValueError):
    """
    Decoding input failed
    """

class HTTPWrongStatus(ValueError):
    """
    Raised when the response of an HTTP request to a Solr Server contains an
    invalid status value (different than 200)
    """

class SolrResponseError(Exception):
    """
    Raised when a problem decoding a Solr Response is found.
    """

class HTTPRequestError(Exception):
    """
    Raised when a problem is found when performing a request to Solr
    """
