import random
import string

from twisted.trial import unittest
from twisted.internet import defer

from txsolr.client import SolrClient
from txsolr.errors import WrongHTTPStatus

# FIXME: avoid hardcoded url
SOLR_URL = 'http://localhost:8983/solr/'


class ConnectionTestCase(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    def test_requestPing(self):
        return self.client.ping()

    def test_requestStatus(self):
        result = defer.Deferred()

        d = self.client._request('GET', '', {}, None)

        def cb(response):
            result.errback('Request should fail')
        d.addCallback(cb)

        def er(failure):
            if isinstance(failure.value, WrongHTTPStatus):
                result.callback(None)
            else:
                result.errback('Got wrong failure')
        d.addErrback(er)

        return result

    def test_addRequest(self):
        return self.client.add(dict(id=1))

    def test_deleteRequest(self):
        return self.client.delete(1)

    def test_deleteByQueryRequest(self):
        return self.client.deleteByQuery('*:*')

    def test_rollbackRequest(self):
        yield self.client.rollback()

    def test_commitRequest(self):
        return self.client.commit()

    def test_optimizeRequest(self):
        return self.client.optimize()

    def test_searchRequest(self):
        return self.client.search('sample')


class AddingDocumentsTestCase(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    def test_addOneDocument(self):
        pass

    def test_addOneDocumentMultipleFields(self):
        pass

    def test_addManyDocuments(self):
        pass

    def test_addUnicodeDocument(self):
        pass

    def test_addDocumentWithNoneField(self):
        pass

    def test_addDocumentWithDatetime(self):
        pass


class UpdatingDocumentsTestCase(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    def test_updateOneDocument(self):
        pass

    def test_updateManyDocuments(self):
        pass


class DeletingDocumentsTestCase(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    def test_deleteOneDocumentByID(self):
        pass

    def test_deleteManyDocumentsByID(self):
        pass

    def test_deleteOneDocumentByQuery(self):
        pass

    def test_deleteManyDocumentsByQuery(self):
        pass


class QueryingDocumentsTestCase(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)
        #Add documents here

    def test_simpleQuery(self):
        pass

    def test_queryWithFields(self):
        pass

    def test_queryWithScore(self):
        pass

    def test_queryWithHighLight(self):
        pass

    def test_queryWithSort(self):
        pass

    def test_queryWithFacet(self):
        pass

    def tearDown(self):
        # Remove documents here
        pass


class CommitingOptimizingTestCase(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    def test_commit(self):
        pass

    def tests_optimize(self):
        pass
