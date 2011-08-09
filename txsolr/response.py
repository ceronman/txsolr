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
Solr Responses.

This module contains classes for parsing responses from the Solr server.
Additionally, it contains Consumer classes for getting a page body in HTTP
Requests.
"""
import logging
import json

from twisted.internet.protocol import Protocol
from twisted.web.client import ResponseDone

from txsolr.errors import SolrResponseError


__all__ = ['ResponseConsumer', 'EmptyResponseConsumer', 'QueryResults',
           'SolrResponse', 'JSONSolrResponse']


_logger = logging.getLogger('txsolr')


class ResponseConsumer(Protocol):
    """
    This class implements a Consumer used to get a body from an HTTP request.
    The consumer is used with the L{twisted.web.client.Agent} class.

    This base consumer gets the body and stores it in memory. For large bodies,
    you should implement your own consumer that use some kind of disk storage.

    The consumer should implement a Twisted L{Protocol}.

    @param deferred: A L{Deferred} that will be fired when all the body is
        consumed.
    @param responseClass: A L{SolrResponse} subclass able to parse the body.
    """

    def __init__(self, deferred, responseClass):
        self.body = ''
        self.deferred = deferred
        self.responseClass = responseClass

    def dataReceived(self, bytes):
        _logger.debug('Consumer data received:\n' + bytes)
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
    """
    This is a Consumer that does nothing. This is used for cases when we don't
    want to consume the body of an HTTP response. For example, when we find a
    wrong status code in the header.
    """

    def dataReceived(self, bytes):
        _logger.debug('Consumer data received:\n' + bytes)


class QueryResults(object):
    """
    This is a simple class used to store the results of a query in a Solr
    Response.

    @ivar numFound: The number of documents found.
    @ivar start: An C{int} representing the first document shown.
        Used for pagination.
    @ivar: docs: A C{dict} representing the documents found.
    """

    def __init__(self, numFound, start, docs):
        self.numFound = numFound
        self.start = start
        self.docs = docs


class SolrResponse(object):
    """Used to represent a response given by a request to a Solr server.

    You should create different subclasses for different response formats.
    Additionally, a SolrResponse may contains properties for other parts of the
    response depending on the parameters of the request. For example a
    'highlighting' property could be added if the query contains highlighting
    parameters.

    @cvar: decoder: An object with a C{decode} method able to decode a raw
        response in a given format.
    @ivar responseDict: The full response as a dict. This is usefull when you
        need an object very similar to the real response issued by the server
    @ivar header: The header of the response. This is usually represented as
        'responseHeader' in the response.
    @ivar results: If this is a response of a query request, it will return the
        results represented by a L{QueryResults}

    @param response: The raw response to be decoded.
    """

    decoder = None

    def __init__(self, response):
        assert self.decoder is not None

        self.responseDict = None
        self.header = None
        self.results = None

        self.rawResponse = response
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
        return 'SolrResponse: %r' % self.rawResponse


class JSONSolrResponse(SolrResponse):
    """
    A SolrResponse that use a JSON Decoder. This decoder should be used when a
    JSON response writer is requested to Solr.
    """

    decoder = json.JSONDecoder()
