import random
import string

from twisted.trial import unittest
from twisted.internet import defer

from txsolr.client import SolrClient
from txsolr.errors import WrongHTTPStatus

# FIXME: avoid hardcoded url
SOLR_URL = 'http://localhost:8983/solr/'

class ClientTest(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    def test_requestPing(self):
        return self.client._request('GET', '/admin/ping', {}, None)

    @defer.inlineCallbacks
    def test_wrongResponseRequest(self):

        try:
            yield self.client._request('GET', '', {}, None)
            defer.returnValue(None)
        except WrongHTTPStatus:
            pass

    @defer.inlineCallbacks
    def test_addRequest(self):
        document = {'id': 1, 'text': 'manuel ceron'}
        yield self.client.add(document)

        documents = [ {'id': 1, 'text': 'manuel ceron'},
                      {'id': 2, 'text': 'fluidinfo'},
                      {'id': 3, 'text': 'solr'} ]

        yield self.client.add(documents)
        yield self.client.add(tuple(documents))
        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_deleteRequest(self):
        id = 1
        yield self.client.delete(id)

        ids = [1, 2, 3, 4]
        yield self.client.delete(ids)
        yield self.client.delete(tuple(ids))
        yield self.client.delete(set(ids))
        defer.returnValue(None)

    def test_deleteByQueryRequest(self):
        return self.client.deleteByQuery('*:*')

    def test_commitRequest(self):
        return self.client.commit()

    def test_rollbackRequest(self):
        yield self.client.rollback()

    def test_optimizeRequest(self):
        return self.client.optimize()

    @defer.inlineCallbacks
    def test_select(self):
        params = dict(q='manuel', wt='json', indent='true')
        yield self.client._select(params)

        longQuery = ''.join(random.choice(string.letters) for _ in range(2000))
        params.update(q=longQuery)
        yield self.client._select(params)

        defer.returnValue(None)

