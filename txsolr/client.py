"""
Solr Client for Twisted
"""

import logging
import urllib

try:
    import json # python >= 2.6
except ImportError:
    import simplejson as json # python < 2.6

from twisted.internet import reactor, defer
from twisted.internet.protocol import Protocol
from twisted.web.client import Agent, ResponseDone
from twisted.web.http_headers import Headers

from txsolr.input import SimpleXMLInputFactory, StringProducer
from txsolr.errors import WrongHTTPStatus, SolrResponseError

_logger = logging.getLogger('txsolr')

class _SimpleJSONConsumer(Protocol):
    def __init__(self, deferred):
        self.body = ''
        self.deferred = deferred

    def dataReceived(self, bytes):
        self.body += bytes

    def connectionLost(self, reason):
        if not isinstance(reason.value, ResponseDone):
            _logger.warning('unclean response: ' + repr(reason.value))

        try:
            response = json.loads(self.body)
        except ValueError:
            msg = 'Unable to decode response using json:\n%s' % self.body
            self.deferred.errback(SolrResponseError(msg))
            return
        try:
            status = response[u'responseHeader'][u'status']
        except KeyError:
            msg = 'Response does not contain status:\n%s' % self.body
            self.deferred.errback(SolrResponseError(msg))
            return

        if status != 0:
            msg = 'Response with invalid status code:\n%s' % self.body
            self.deferred.errback(SolrResponseError(msg))
            return

        self.deferred.callback(response)

class _EmptyConsumer(Protocol):

    def conectionMade(self, bytes):
        self.transport.stopProducing()

class SolrJSONDecoder(json.JSONDecoder):
    wt = 'json'

class SolrClient(object):
    """
    A Solr Client. Used to make requests to a Solr Server
    """

    def __init__(self, url, inputFactory=None):
        """
        Creates the Solr Client object

        @param url: the url of the Solr server
        @param inputFactory: a class that is going to produce the input for the
                             server, by default uses a simple input creator
        """

        self.url = url.rstrip('/')
        if inputFactory == None:
            self.inputFactory = SimpleXMLInputFactory()

        self._agent = Agent(reactor)

    def _request(self, method, path, headers, bodyProducer):
        result = defer.Deferred()

        url = self.url + path
        headers.update({'User-Agent': ['txSolr']})
        headers = Headers(headers)
        _logger.debug('Requesting: [%s] %s' % (method, url))
        d = self._agent.request(method, url, headers, bodyProducer)

        def responseCallback(response):
            _logger.debug('Received response from ' + url)
            if response.code != 200:
                deliveryProtocol = _EmptyConsumer()
                response.deliverBody(deliveryProtocol)
                result.errback(WrongHTTPStatus(response.code))
            else:
                deliveryProtocol = _SimpleJSONConsumer(result)
                response.deliverBody(deliveryProtocol)
        d.addCallback(responseCallback)

        def responseErrback(failure):
            result.errback(failure.value)
        d.addErrback(responseErrback)

        return result

    def _update(self, input):
        method = 'POST'
        path = '/update?wt=json'
        headers = { 'Content-Type': [self.inputFactory.contentType] }
        return self._request(method, path, headers, input)

    def _select(self, params):
        # force JSON response for now
        params.update(wt='json')

        query = urllib.urlencode(params)

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

    # TODO: add parameters: overwrite, commitWithin and boost for docs and
    # Fields
    def add(self, documents):
        input = self.inputFactory.createAdd(documents)
        return self._update(input)

    def delete(self, ids):
        input = self.inputFactory.createDelete(ids)
        return self._update(input)

    def deleteByQuery(self, query):
        input = self.inputFactory.createDelete(query)
        return self._update(input)

    # TODO: add parameters
    def commit(self):
        input = self.inputFactory.createCommit()
        return self._update(input)

    def rollback(self):
        input = self.inputFactory.createRollback()
        return self._update(input)

    # TODO: add parameters
    def optimize(self):
        input = self.inputFactory.createOptimize()
        return self._update(input)

    def search(self, query, params):
        params.update(q=query.encode('UTF-8'))

if __name__ == '__main__':
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    c = SolrClient('http://localhost:8983/solr/')
    document = {'id': 1, 'text': 'manuel ceron'}
#    d = c.add([document])
#    d = c.rollback()
#    d = c.delete(1000)
    d = c._select({'q': 'manuel'.encode('UTF-8'), 'indent': 'true'})

    def cb(content):
        print 'Delivery:'
        print content
    d.addCallback(cb)

    def er(failure):
        print 'Error:'
        print failure.getErrorMessage()
        print failure.getTraceback()
    d.addErrback(er)

    d.addBoth(lambda _: reactor.stop())
    reactor.run()
