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

    def test_add(self):
        document = {'id': 1, 'text': 'manuel ceron'}
        return self.client.add([document])
