"""
Solr Client for Twisted
"""

import logging
import urllib

from twisted.internet import reactor, defer
from twisted.web.client import Agent
from twisted.web.http_headers import Headers

from txsolr.input import SimpleXMLInputFactory, StringProducer
from txsolr.response import (ResponseConsumer, EmptyResponseConsumer,
                             JSONSolrResponse)
from txsolr.errors import WrongHTTPStatus

_logger = logging.getLogger('txsolr')


# TODO: decouple from JSON
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
                deliveryProtocol = EmptyResponseConsumer()
                response.deliverBody(deliveryProtocol)
                result.errback(WrongHTTPStatus(response.code))
            else:
                deliveryProtocol = ResponseConsumer(result, JSONSolrResponse)
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
        _logger.debug('Updating:\n%s' % input.body)
        return self._request(method, path, headers, input)

    def _select(self, params):
        # force JSON response for now
        params.update(wt='json')

        # Some solr params contains dots (i.e: ht.fl) We use underscores and
        # replace
        params = dict((key.replace('_', '.'), value)
                      for key, value in params.iteritems())

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

    def add(self, documents, overwrite=None, commitWithin=None):
        input = self.inputFactory.createAdd(documents, overwrite, commitWithin)
        return self._update(input)

    def delete(self, ids):
        input = self.inputFactory.createDelete(ids)
        return self._update(input)

    def deleteByQuery(self, query):
        input = self.inputFactory.createDeleteByQuery(query)
        return self._update(input)

    # TODO: add parameters
    def commit(self, waitFlush=None, waitSearcher=None, expungeDeletes=None):
        input = self.inputFactory.createCommit(waitFlush,
                                               waitSearcher,
                                               waitSearcher)
        return self._update(input)

    def rollback(self):
        input = self.inputFactory.createRollback()
        return self._update(input)

    # TODO: add parameters
    def optimize(self):
        input = self.inputFactory.createOptimize()
        return self._update(input)

    def search(self, query, **kwargs):
        params = {}
        params.update(kwargs)
        params.update(q=query.encode('UTF-8'))
        return self._select(params)

    def ping(self):
        method = 'GET'
        path = '/admin/ping?wt=json'
        headers = {}
        return self._request(method, path, headers, None)

if __name__ == '__main__':
#    import sys
#    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    import txsolr
    txsolr.logToStderr()
    c = SolrClient('http://localhost:8983/solr/')
    document = {'id': 1, 'text': 'manuel tres', 'name': 'tres'}
#    d = c.add([document])
    d = c.commit()
#    d = c.rollback()
#    d = c.delete(1000)
#    d = c._select({'q': 'manuel'.encode('UTF-8'), 'indent': 'true'})
#    d = c.search('manuel')
#    d = c.deleteByQuery('*:*')

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
