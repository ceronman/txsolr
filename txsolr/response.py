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

