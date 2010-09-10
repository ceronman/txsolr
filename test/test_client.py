# -*- coding: utf-8 -*-

import random
import string
import datetime
import pprint

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

        docs = []
        for _ in range(5):
            doc = {'id': _randomString(20),
                   'name': name,
                   'title': [_randomString(20)]}
            docs.append(doc)

        yield self.client.add(docs)
        yield self.client.commit()

        r = yield self.client.search('name:%s' % name)

        self.assertEqual(r.response.numFound, len(docs),
                         'Document was not added')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_addUnicodeDocument(self):
        doc = {'id': _randomString(20),
               'title': [u'カカシ外伝～戦場のボーイズライフ ☝☜']}

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

        doc = r.response.docs[0]

        # FIXME: dates proably should be parsed to datetime objects

        self.assertEqual(doc.test1_dt, u'2010-01-01T23:59:59Z',
                         'Datetime value does not match')
        self.assertEqual(doc.test2_dt, u'2010-01-01T00:00:00Z',
                         'Date value does not match')

        defer.returnValue(None)


class UpdatingDocumentsTestCase(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    @defer.inlineCallbacks
    def test_updateOneDocument(self):
        data = _randomString(20)
        updated_data = _randomString(20)

        doc = {'id': _randomString(20),
               'test_s': data}

        # add initial data
        yield self.client.add(doc)
        yield self.client.commit()


        # update data
        doc['test_s'] = updated_data
        yield self.client.add(doc)
        yield self.client.commit()

        r = yield self.client.search('id:%s' % doc['id'])
        self.assertEqual(r.response.docs[0].test_s, updated_data,
                         'Update did not work')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_updateManyDocuments(self):

        data = _randomString(20)
        updated_data = _randomString(20)

        docs = []
        for _ in range(5):
            doc = {'id': _randomString(20),
                   'test_s': data }
            docs.append(doc)

        # add initial data
        yield self.client.add(docs)
        yield self.client.commit()

        # update data
        for doc in docs:
            doc['test_s'] = updated_data

        yield self.client.add(docs)
        yield self.client.commit()

        for doc in docs:
            r = yield self.client.search('id:%s' % doc['id'])
            self.assertEqual(r.response.docs[0].test_s, updated_data,
                             'Multiple update did not work')

        defer.returnValue(None)


class DeletingDocumentsTestCase(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    @defer.inlineCallbacks
    def test_deleteOneDocumentByID(self):

        doc = {'id': _randomString(20),
               'name': _randomString(20)}

        # Fist add the document
        yield self.client.add(doc)
        yield self.client.commit()

        # Next delete the document
        yield self.client.delete(doc['id'])
        yield self.client.commit()

        r = yield self.client.search('id:%s' % doc['id'])

        self.assertEqual(r.response.numFound, 0,
                         "The document was not deleted")

        r = yield self.client.search('name:%s' % doc['name'])

        self.assertEqual(r.response.numFound, 0,
                         "The document was not deleted")

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_deleteManyDocumentsByID(self):

        name = _randomString(20)

        docs = []
        for _ in range(5):
            doc = {'id': _randomString(20),
                   'name': name}
            docs.append(doc)

        # Add the documents
        yield self.client.add(docs)
        yield self.client.commit()

        # Delete the documents
        ids = [doc['id'] for doc in docs]
        yield self.client.delete(ids)
        yield self.client.commit()

        r = yield self.client.search('name:%s' % name)
        self.assertEqual(r.response.numFound, 0,
                         'Document was not deleted')

        for doc in docs:
            r = yield self.client.search('id:%s' % doc['id'])
            self.assertEqual(r.response.numFound, 0,
                             'Document was not deleted')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_deleteOneDocumentByQuery(self):

        doc = {'id': _randomString(20),
               'name': _randomString(20)}

        # Fist add the document
        yield self.client.add(doc)
        yield self.client.commit()

        # Next delete the document
        yield self.client.deleteByQuery('id:%s' % doc['id'])
        yield self.client.commit()

        r = yield self.client.search('id:%s' % doc['id'])

        self.assertEqual(r.response.numFound, 0,
                         "The document was not deleted")

        r = yield self.client.search('name:%s' % doc['name'])

        self.assertEqual(r.response.numFound, 0,
                         "The document was not deleted")

        defer.returnValue(None)

    def test_deleteManyDocumentsByQuery(self):

        name = _randomString(20)

        docs = []
        for _ in range(5):
            doc = {'id': _randomString(20),
                   'name': name}
            docs.append(doc)

        # Add the documents
        yield self.client.add(docs)
        yield self.client.commit()

        # Delete the documents
        yield self.client.deleteByQuery('name:%s' % name)
        yield self.client.commit()

        r = yield self.client.search('name:%s' % name)
        self.assertEqual(r.response.numFound, 0,
                         'Document was not deleted')

        for doc in docs:
            r = yield self.client.search('id:%s' % doc['id'])
            self.assertEqual(r.response.numFound, 0,
                             'Document was not deleted')

        defer.returnValue(None)


class QueryingDocumentsTestCase(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.client = SolrClient(SOLR_URL)

        # Test documents used for querying
        self.narutoId = _randomString(20)
        self.bleachId = _randomString(20)
        self.deathnoteId = _randomString(20)

        self.documents = [
            {'id': self.narutoId,
             'title':  u'Naruto',
             'links': ['http://en.wikipedia.org/wiki/Naruto'],
             'category': 'action comedy drama fantasy',
             'popularity': 10,
             'info_t': (u'Naruto (NARUTO—ナルト—?, romanized as NARUTO) '
                        u'is an ongoing Japanese manga series written '
                        u'and illustrated by Masashi Kishimoto. The '
                        u'plot tells the story of Naruto Uzumaki, '
                        u'an adolescent ninja who constantly searches '
                        u'for recognition and aspires to become a Hokage, '
                        u'the ninja in his village that is acknowledged '
                        u'as the leader and the strongest of all.')},

            {'id': self.bleachId,
             'title':  u'Bleach',
             'category': 'action comedy drama supernatural',
             'links': ['http://en.wikipedia.org/wiki/Bleach_(manga)'],
             'popularity': 7,
             'info_t': (u'Bleach (ブリーチ Burīchi?, Romanized as BLEACH '
                        u'in Japan) is a Japanese manga series written '
                        u'and illustrated by Tite Kubo. Bleach follows '
                        u'the adventures of Ichigo Kurosaki after he '
                        u'obtains the powers of a Soul Reaper - a death '
                        u'personification similar to the Grim Reaper - '
                        u'from Rukia Kuchiki.')},

             {'id': self.deathnoteId,
             'title':  u'Death Note',
             'category': 'drama mystery psychological supernatural thriller',
             'links': ['http://en.wikipedia.org/wiki/Death_Note'],
             'popularity': 8,
             'info_t': (u'Death Note (デスノート Desu Nōto?) is a manga '
                        u'series created by writer Tsugumi Ohba and '
                        u'manga artist Takeshi Obata. The main character '
                        u'is Light Yagami, a high school student who '
                        u'discovers a supernatural notebook, the "Death '
                        u'Note", dropped on Earth by a death god '
                        u'named Ryuk.')},
        ]

        yield self.client.add(self.documents)
        yield self.client.commit()

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_simpleQuery(self):

        r = yield self.client.search('title:Bleach OR title:"Death Note"')

        self.assertEqual(r.response.numFound, 2,
                         'Wrong numFound after query')

        for doc in r.response.docs:
            self.assertTrue(doc.id in (self.bleachId, self.deathnoteId),
                            'Document found does not match with added one')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_unicodeQuery(self):

        r = yield self.client.search(u'info_t:ブリーチ') # Bleach in Japanese

        self.assertEqual(r.response.numFound, 1,
                         'Wrong numFound after query')

        doc = r.response.docs[0]
        self.assertEqual(doc.id, self.bleachId,
                        'Document found does not match with added one')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_queryWithFields(self):

        # Fist test query with a single field
        r = yield self.client.search('info_t:manga', fl='links')
        for doc in r.response.docs:
            self.assertTrue(hasattr(doc, 'links'),
                           'Results do not have specified field')

            self.assertFalse(hasattr(doc, 'id'),
                             'Results have unrequested fields')

            self.assertFalse(hasattr(doc, 'info_t'),
                             'Results have unrequested fields')

            self.assertFalse(hasattr(doc, 'popularity'),
                             'Results have unrequested fields')

        # Test query with multiple fields
        r = yield self.client.search('info_t:manga', fl='links,popularity')
        for doc in r.response.docs:
            self.assertTrue(hasattr(doc, 'links'),
                           'Results do not have specified field')

            self.assertFalse(hasattr(doc, 'id'),
                             'Results have unrequested fields')

            self.assertFalse(hasattr(doc, 'info_t'),
                             'Results have unrequested fields')

            self.assertTrue(hasattr(doc, 'popularity'),
                             'Results do not have specified field')

        # Test query with all fields
        r = yield self.client.search('info_t:manga', fl='*')
        for doc in r.response.docs:
            self.assertTrue(hasattr(doc, 'links'),
                            'Results do not have specified field')

            self.assertTrue(hasattr(doc, 'id'),
                            'Results do not have specified field')

            self.assertTrue(hasattr(doc, 'info_t'),
                            'Results do not have specified field')

            self.assertTrue(hasattr(doc, 'popularity'),
                            'Results do not have specified field')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_queryWithScore(self):
        r = yield self.client.search('info_t:manga', fl='id,score')
        for doc in r.response.docs:
            self.assertTrue(hasattr(doc, 'id'),
                           'Results do not have ID field')

            self.assertTrue(hasattr(doc, 'score'),
                           'Results do not have score')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_queryWithHighlight(self):

        # TODO: poor test. Improve it

        r = yield self.client.search('info_t:manga',
                                     hl='true',
                                     hl_fl='info_t')

        # FIXME: this tests shows a potential problem with response system
        #        response objects should be changed

        self.assertTrue(hasattr(r, 'highlighting'))

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_queryWithSort(self):
        r = yield self.client.search('info_t:manga', sort='popularity desc')
        docs = r.response.docs

        self.assertEqual(docs[0].id, self.narutoId,
                         'Wrong sorting order')

        self.assertEqual(docs[1].id, self.deathnoteId,
                         'Wrong sorting order')

        self.assertEqual(docs[2].id, self.bleachId,
                         'Wrong sorting order')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_queryWithFacet(self):

        # TODO: poor test. Improve it

        # field facet
        r = yield self.client.search('info_t:manga',
                                     facet = 'true',
                                     facet_field = 'category')


        category_facet = r.facet_counts.facet_fields.category

        self.assertEqual(len(category_facet), 16, 'Unexpected facet')

        # query facet
        # FIXME: current api does not allow multiple facet queries or fields
        r = yield self.client.search('info_t:manga',
                                     facet = 'true',
                                     facet_query = 'popularity:[0 TO 8]')

        # FIXME: rawResponse should not be needed here
        facet_queries = r.rawResponse['facet_counts']['facet_queries']

        self.assertEqual(len(facet_queries), 1, 'Unexpected facet')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def tearDown(self):
        ids = [doc['id'] for doc in self.documents]

        yield self.client.delete(ids)
        yield self.client.commit()

        defer.returnValue(None)


class CommitingTestCase(unittest.TestCase):

    def setUp(self):
        self.client = SolrClient(SOLR_URL)

    @defer.inlineCallbacks
    def test_commit(self):
        doc = {'id': _randomString(20)}

        yield self.client.add(doc)

        r = yield self.client.search('id:%s' % doc['id'])

        self.assertEqual(r.response.numFound, 0,
                         'Document addition was commited')

        yield self.client.commit()

        r = yield self.client.search('id:%s' % doc['id'])

        self.assertEqual(r.response.numFound, 1,
                         'Commit did not work')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def test_rollback(self):

        doc = {'id': _randomString(20)}

        yield self.client.add(doc)
        yield self.client.rollback()
        yield self.client.commit()

        r = yield self.client.search('id:%s' % doc['id'])

        self.assertEqual(r.response.numFound, 0,
                         'Rollback did not work')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def tests_optimize(self):

        doc = {'id': _randomString(20)}

        yield self.client.add(doc)

        r = yield self.client.search('id:%s' % doc['id'])

        self.assertEqual(r.response.numFound, 0,
                         'Document addition was commited')

        yield self.client.optimize()

        r = yield self.client.search('id:%s' % doc['id'])

        self.assertEqual(r.response.numFound, 1,
                         'Optimize did not work')

        defer.returnValue(None)
