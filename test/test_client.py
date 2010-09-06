from twisted.trial import unittest

from txsolr.client import SolrClient

# FIXME: avoid hardcoded url
SOLR_URL = 'http://localhost:8983/solr/'

class ClientTest(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    def test_request(self):
        return self.client._request('GET', '', {}, None)
