import random
import string
import datetime

#import logging
#import sys
#logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

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

    @defer.inlineCallbacks
    def test_requestStatus(self):
        try:
            yield self.client._request('HEAD', '', {}, None)
        except WrongHTTPStatus:
            pass

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

def _randomString(size):

    return ''.join(random.choice(string.letters) for _ in range(size))

class AddingDocumentsTestCase(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    @defer.inlineCallbacks
    def test_addOneDocument(self):

        doc = {'id': _randomString(20)}

        yield self.client.add(doc)
        yield self.client.commit()

        r = yield self.client.search('id:%s' % doc['id'])

        self.assertEqual(r.response.numFound, 1,
                         "Added document not found in the index")

        self.assertEqual(r.response.docs[0].id, doc['id'],
                         "Found ID does not match with added document")

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_addOneDocumentMultipleFields(self):

        name = _randomString(20)
        links = [_randomString(20) for _ in range(5)]

        for seq in (list, tuple, set):
            doc = {'id': _randomString(20),
                   'name': name,
                   'title': _randomString(20),
                   'links': seq(links)}

            yield self.client.add(doc)

        yield self.client.commit()

        r = yield self.client.search('name:%s' % name)

        self.assertEqual(r.response.numFound, 3,
                         "Did not get expected results")

        for doc in r.response.docs:
            self.assertTrue(doc.links, links)

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_addManyDocuments(self):

        name = _randomString(20)
        LENGTH = 5

        docs = []
        for _ in range(LENGTH):
            doc = {'id': _randomString(20),
                   'name': name,
                   'title': [_randomString(20)]}
            docs.append(doc)

        yield self.client.add(docs)
        yield self.client.commit()

        r = yield self.client.search('name:%s' % name)

        self.assertEqual(r.response.numFound, LENGTH)

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_addUnicodeDocument(self):
        doc = {'id': _randomString(20),
               'title': [unicode(_randomString(20))]}

        yield self.client.add(doc)
        yield self.client.commit()

        r = yield self.client.search('id:%s' % doc['id'])

        self.assertEqual(r.response.docs[0].title, doc['title'],
                         "Unicode value does not match with found document")

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_addDocumentWithNoneField(self):
        doc = {'id': _randomString(20),
               'title': None}

        yield self.client.add(doc)
        yield self.client.commit()

        r = yield self.client.search('id:%s' % doc['id'])

        self.assertRaises(AttributeError, getattr, r.response.docs[0], 'title')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_addDocumentWithDatetime(self):

        # NOTE: Microseconds are ignored by Solr

        doc = {'id': _randomString(20),
               'test1_dt': datetime.datetime(2010, 1, 1, 23, 59, 59, 999),
               'test2_dt': datetime.date(2010, 1, 1)}

        yield self.client.add(doc)
        yield self.client.commit()

        r = yield self.client.search('id:%s' % doc['id'])

        print r.rawResponse

        doc = r.response.docs[0]

        self.assertEqual(doc.test1_dt, u'2010-01-01T23:59:59Z',
                         'Datetime value does not match')
        self.assertEqual(doc.test2_dt, u'2010-01-01T00:00:00Z',
                         'Date value does not match')

        defer.returnValue(None)


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
