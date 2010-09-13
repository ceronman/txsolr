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
Solr Response Decoder
"""

import logging

try:
    import json #@UnusedImport for Python >= 2.6
except:
    import simplejson as json #@Reimport for Python < 2.6

from twisted.internet.protocol import Protocol
from twisted.web.client import ResponseDone

from txsolr.errors import SolrResponseError


__all__ = ['ResponseConsumer', 'EmptyResponseConsumer', 'QueryResults',
           'SolrResponse', 'JSONSolrResponse']


_logger = logging.getLogger('txsolr')

class ResponseConsumer(Protocol):
    def __init__(self, deferred, responseClass):
        self.body = ''
        self.deferred = deferred
        self.responseClass=responseClass

    def dataReceived(self, bytes):
        self.body += bytes

    def connectionLost(self, reason):
        if not isinstance(reason.value, ResponseDone):
            _logger.warning('unclean response: ' + repr(reason.value))

        try:
            response = self.responseClass(self.body)
        except SolrResponseError, e:
            self.errback(e)

        self.deferred.callback(response)


class EmptyResponseConsumer(Protocol):

    def conectionMade(self, bytes):
        self.transport.stopProducing()


class QueryResults(object):

    def __init__(self, numFound, start, docs):
        self.numFound = numFound
        self.start = start
        self.docs = docs


class SolrResponse(object):

    decoder = None

    def __init__(self, response):
        assert self.decoder is not None

        self.responseDict = None
        self.header = None
        self.results = None

        self.responseDict = self._decodeResponse(response)
        self._update()

    def _update(self):

        response = self.responseDict

        if not 'responseHeader' in response:
            raise SolrResponseError('Response does not have header')

        self.header = response['responseHeader']

        if not 'status' in self.header:
            raise SolrResponseError('Response does not have status')

        if self.header['status'] != 0:
            raise SolrResponseError('Response status != 0')

        if 'response' in response:
            try:
                self.results = QueryResults(response['response']['numFound'],
                                       response['response']['start'],
                                       response['response']['docs'])
            except KeyError:
                raise SolrResponseError('Wrong results')

        for key, value in response.iteritems():
            if key in ('response', 'responseHeader'):
                continue

            setattr(self, key, value)


    def _decodeResponse(self, response):
        try:
            return self.decoder.decode(response)
        except ValueError:
            msg = 'Unable to use %s to decode %s' % (repr(self.decoder),
                                                     response)
            raise SolrResponseError(msg)

    def __repr__(self):
        return 'SolrResponse: %s' % repr(self.rawResponse)


class JSONSolrResponse(SolrResponse):

    decoder = json.JSONDecoder()

