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
txSolr is a Python client for the Solr Enterprise Search Server. It has been
designed to be used with the Twisted asynchronous networking library.

txSolr can be used to add, update, delete and query documents in a Solr
instance. All operations return Twisted's deferreds for asynchronous
programming
"""

import logging
import urllib

from twisted.internet import reactor, defer
from twisted.web.client import Agent
from twisted.web.http_headers import Headers

from txsolr.input import SimpleXMLInputFactory, StringProducer
from txsolr.errors import HTTPWrongStatus, HTTPRequestError
from txsolr.response import (ResponseConsumer, EmptyResponseConsumer,
                             JSONSolrResponse)


__all__ = ['SolrClient']


_logger = logging.getLogger('txsolr')


# TODO: decouple from JSON
class SolrClient(object):
    """
    Solr client class used to perform requests to a Solr instance.

    The client can use different input and output methods using Twisted
    IProducer and IConsumer.
    """

    def __init__(self, url, inputFactory=None):
        """
        Creates the Solr Client object

        @param url: The URL of the Solr server

        @param inputFactory: The input body generator. For advanced uses this
        argument could be used to create custom body generators for the requests
        using Twisted's IProducer
        """

        self.url = url.rstrip('/')
        if inputFactory == None:
            self.inputFactory = SimpleXMLInputFactory()

    def _request(self, method, path, headers, bodyProducer):
        """
        Performs a request to a Solr client using twisted.web.client.Agent. The
        request examines the response to look for wrong header status.
        Additionally, it parses the response using a ResponseConsumer and
        creates a SolrResponse which will be given to the returning deferred
        callback
        """

        result = defer.Deferred()

        url = self.url + path
        headers.update({'User-Agent': ['txSolr']})
        headers = Headers(headers)
        _logger.debug('Requesting: [%s] %s' % (method, url))
        agent = Agent(reactor)
        d = agent.request(method, url, headers, bodyProducer)

        def responseCallback(response):
            _logger.debug('Received response from ' + url)
            if response.code != 200:
                deliveryProtocol = EmptyResponseConsumer()
                response.deliverBody(deliveryProtocol)
                result.errback(HTTPWrongStatus(response.code))
            else:
                deliveryProtocol = ResponseConsumer(result, JSONSolrResponse)
                response.deliverBody(deliveryProtocol)
        d.addCallback(responseCallback)

        def responseErrback(failure):
            result.errback(HTTPRequestError(failure))
        d.addErrback(responseErrback)

        return result

    def _update(self, input):
        """
        Performs a request to the /update method fo the Solr server
        """

        method = 'POST'
        path = '/update?wt=json'
        headers = { 'Content-Type': [self.inputFactory.contentType] }
        _logger.debug('Updating:\n%s' % input.body)
        return self._request(method, path, headers, input)

    def _select(self, params):
        """
        Performs a request to the /select method fo the Solr server
        """

        # force JSON response for now
        params.update(wt=u'json')

        
        encodedParameters = {}
        for key, value in params.iteritems():
            # Some solr params contains dots (i.e: ht.fl) We use underscores.
            key = key.replace('_', '.')
            if isinstance(value, unicode):
                value = value.encode('UTF-8')
            encodedParameters[key] = value

        query = urllib.urlencode(encodedParameters)

        if len(query) < 1024:
            method = 'GET'
            path = '/select' + '?' + query
            headers = {}
            input = None
        else:
            method = 'POST'
            path = '/select'
            headers = { 'Content-type': ['application/x-www-form-urlencoded'] }
            input = StringProducer(query)

        return self._request(method, path, headers, input)

    def add(self, documents, overwrite=None, commitWithin=None):
        """
        Add one or many documents to a Solr Instance

        @return: a deferred which will callback a SolrResponse as parameter

        @param documents: A dict or list of dicts representing the documents.
        The dict's keys should be field names and the values field content.

        @param overwrite: newer documents will replace previously added
        documents with the same uniqueKey.

        @param commitWithin: the addition will be committed within that time
        """

        input = self.inputFactory.createAdd(documents, overwrite, commitWithin)
        return self._update(input)

    def delete(self, ids):
        """
        Delete one or many documents given the ID or IDs of the documents

        @return: a deferred which will callback a SolrResponse as parameter

        @param ids: a string or list of string representing the IDs of the
        documents that will be deleted
        """

        input = self.inputFactory.createDelete(ids)
        return self._update(input)

    def deleteByQuery(self, query):
        """
        Delete all documents returned by a query

        @return: a deferred which will callback a SolrResponse as parameter

        @param query: A query that returns the documents to be deleted.
        """

        input = self.inputFactory.createDeleteByQuery(query)
        return self._update(input)

    def commit(self, waitFlush=None, waitSearcher=None, expungeDeletes=None):
        """
        Issues a commit action to the Solr Server

        @return: a deferred which will callback a SolrResponse as parameter

        @param waitFlush: Server will block until index changes are flushed to
        disk

        @param waitSearcher: Server will  block until a new searcher is opened
        and registered as the main query searchers

        @param expungeDeletes: merge segments with deletes away
        """

        input = self.inputFactory.createCommit(waitFlush,
                                               waitSearcher,
                                               expungeDeletes)
        return self._update(input)

    def rollback(self):
        """
        Withdraw all uncommitted changes

        @return: a deferred which will callback a SolrResponse as parameter
        """

        input = self.inputFactory.createRollback()
        return self._update(input)

    def optimize(self, waitFlush=None, waitSearcher=None, maxSegments=None):
        """
        Issues a commit action to the Solr Server

        @return: a deferred which will callback a SolrResponse as parameter

        @param waitFlush: Server will block until index changes are flushed
        to disk

        @param waitSearcher: Server will  block until a new searcher is opened
        and registered as the main query searcher

        @param maxSegments: optimizes down to at most this number of segments
        """

        input = self.inputFactory.createOptimize(waitFlush,
                                                 waitSearcher,
                                                 maxSegments)
        return self._update(input)

    def search(self, query, **kwargs):
        """
        Performs a query in the Solr Server

        @return: a deferred which will callback a SolrResponse as parameter. The
        results member of the response will contain information about the search
        results (see: SolrResponse)

        @param query: The SolrQuery

        @param *kwargs: Additional parameters for the server. For instance:
        'hl' for highlighting, 'sort' for sorting, etc. See Solr documentation
        for all available options
        """

        params = {}
        params.update(kwargs)
        params.update(q=query)
        return self._select(params)

    def ping(self):
        """
        Just ping the server to know if it's alive.

        @return: a deferred which will callback a SolrResponse as parameter
        """
        method = 'GET'
        path = '/admin/ping?wt=json'
        headers = {}
        return self._request(method, path, headers, None)
