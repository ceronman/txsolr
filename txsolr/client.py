"""
Solr Client for Twisted
"""

from twisted.internet import reactor, defer
from twisted.internet.protocol import Protocol
from twisted.web.client import Agent, ResponseDone
from twisted.web.http_headers import Headers
from twisted.python import log

from txsolr.input import SimpleXMLInputFactory
from txsolr.errors import WrongResponseCode


class _StringConsumer(Protocol):
    def __init__(self, deferred):
        self.body = ''
        self.deferred = deferred

    def dataReceived(self, bytes):
        self.body += bytes

    def connectionLost(self, reason):
        if not isinstance(reason.value, ResponseDone):
            log.msg('Warning: unclean response: ' + str(reason.value))
        self.deferred.callback(self.body)

class _EmptyConsumer(Protocol):

    def conectionMade(self, bytes):
        self.transport.stopProducing()


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
        d = self._agent.request(method, url, headers, bodyProducer)

        def responseCallback(response):
            if response.code != 200:
                deliveryProtocol = _EmptyConsumer()
                response.deliverBody(deliveryProtocol)
                result.errback(WrongResponseCode(response.code))
            else:
                print response.version
                deliveryProtocol = _StringConsumer(result)
                response.deliverBody(deliveryProtocol)
        d.addCallback(responseCallback)

        def responseErrback(failure):
            result.errback(failure.value)
        d.addErrback(responseErrback)

        return result

    def _update(self, input):
        method = 'POST'
        path = '/update'
        headers = { 'Content-Type': [self.inputFactory.contentType] }
        return self._request(method, path, headers, input)

    # TODO: add parameters: overwrite, commitWithin and boost for docs and
    # Fields
    def add(self, documents):
        input = self.inputFactory.createAdd(documents)
        return self._update(input)

if __name__ == '__main__':
    c = SolrClient('http://localhost:8983/solr/')
    document = {'id': 1, 'text': 'manuel ceron'}
    d = c.add([document])

    def cb(content):
        print 'Delivery:'
        print content
    d.addCallback(cb)

    def er(failure):
        print 'Error:'
        print failure.getErrorMessage()
        print failure.getTraceback()
    d.addErrback(er)
    reactor.run()
