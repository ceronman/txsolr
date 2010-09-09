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


def _updateFromDict(obj, dict_):
    """
    Updates and object using a dictionary recursively
    """

    assert isinstance(dict_, dict)

    def convertValue(name, value):
        if isinstance(value, dict):
            obj = type(str(name) + 'Object', (object,), {})
            _updateFromDict(obj, value)
            return obj

        if isinstance(value, (list, tuple)):
            obj = [convertValue(str(name) + 'Item', i)
                   for i in value]
            return obj

        return value

    for key, value in dict_.iteritems():
        assert isinstance(key, basestring)

        setattr(obj, str(key), convertValue(key, value))


class SolrResponse(object):

    decoder = None

    def __init__(self, response):
        assert self.decoder is not None

        self.rawResponse = self._decodeResponse(response)
        self._update()

    def _update(self):
        _updateFromDict(self, self.rawResponse)

        try:
            status = self.responseHeader.status
        except AttributeError:
            msg = 'Status code not found in:\n%s' % self.rawResponse
            raise SolrResponseError(msg)

        if status != 0:
            msg = 'Invalid status code:\n%s' % self.rawResponse
            raise SolrResponseError(msg)

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

