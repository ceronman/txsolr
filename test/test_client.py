from twisted.trial import unittest
from twisted.internet import defer

from txsolr.client import SolrClient
from txsolr.errors import WrongResponseCode

# FIXME: avoid hardcoded url
SOLR_URL = 'http://localhost:8983/solr/'

class ClientTest(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    def test_wrongResponseRequest(self):
        result = defer.Deferred()

        d = self.client._request('GET', '', {}, None)

        def callback(content):
            result.errback('Request should fail')
        d.addCallback(callback)

        def errback(failure):
            if isinstance(failure.value, WrongResponseCode):
                result.callback(None)
            else:
                result.errback('Error should be WrongResponseCode')
        d.addErrback(errback)

        return result

    @defer.inlineCallbacks
    def test_addRequest(self):
        document = {'id': 1, 'text': 'manuel ceron'}
        yield self.client.add(document)

        documents = [ {'id': 1, 'text': 'manuel ceron'},
                      {'id': 2, 'text': 'fluidinfo'},
                      {'id': 3, 'text': 'solr'} ]

        yield self.client.add(documents)
        yield self.client.add(tuple(documents))

    @defer.inlineCallbacks
    def test_deleteRequest(self):
        id = 1
        yield self.client.delete(id)

        ids = [1, 2, 3, 4]
        yield self.client.delete(ids)
        yield self.client.delete(tuple(ids))
        yield self.client.delete(set(ids))

        defer.returnValue(None)

    def test_commitRequest(self):
        return self.client.commit()

    @defer.inlineCallbacks
    def test_rollbackRequest(self):
        yield self.client.commit()
        yield self.client.rollback()
        defer.returnValue(None)

    def test_optimizeRequest(self):
        return self.client.optimize()
