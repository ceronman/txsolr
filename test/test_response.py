import unittest

from txsolr.response import _updateFromDict, JSONSolrResponse
from txsolr.errors import SolrResponseError

class ResponseTest(unittest.TestCase):

    def test_updateFromDict(self):

        dict = {
            'int': 1,
            'str': 'hello',
            'list': [1, 2, 3],
            'dict': { 'a': 1, 'b': 2 },
            'nested': {
                'one': 1,
                'two': (1, 2, 3),
                'three': { 'x': 1, 'y': 2 },
            },
        }

        class A(object):
            pass

        obj = A()

        _updateFromDict(obj, dict)

        self.assertEqual(obj.int, 1)
        self.assertEqual(obj.str, 'hello')
        self.assertEqual(obj.list[0], 1)
        self.assertEqual(obj.list[2], 3)
        self.assertEqual(obj.dict.a, 1)
        self.assertEqual(obj.dict.b, 2)
        self.assertEqual(obj.nested.one, 1)
        self.assertEqual(obj.nested.two[1], 2)
        self.assertEqual(obj.nested.three.x, 1)
        self.assertEqual(obj.nested.three.y, 2)

    def test_jsonSolrResponse(self):

        raw = '''{
                 "responseHeader":{
                  "status":0,
                  "QTime":2,
                  "params":{
                    "indent":"on",
                    "wt":"json",
                    "q":"manuel"}},
                 "response":{"numFound":0,"start":0,"docs":[]}
                 }'''

        r = JSONSolrResponse(raw)

        self.assertEqual(r.responseHeader.status, 0)
        self.assertEqual(r.responseHeader.QTime, 2)
        self.assertEqual(r.response.numFound, 0)
        self.assertTrue(len(r.response.docs) == 0)

        raw = '''{
                 "responseHeader":{
                  "status":1,
                  "QTime":2,
                  "params":{
                    "indent":"on",
                    "wt":"json",
                    "q":"manuel"}},
                 "response":{"numFound":0,"start":0,"docs":[]}
                 }'''

        self.assertRaises(SolrResponseError, JSONSolrResponse, raw)

        raw = '''<response>
                    <lst name="responseHeader">
                     <int name="status">0</int>
                     <int name="QTime">0</int>
                     <lst name="params">
                      <str name="indent">on</str>
                      <str name="wt">standard</str>
                      <str name="q">manuel</str>
                     </lst>
                    </lst>
                    <result name="response" numFound="0" start="0"/>
                </response> '''

        self.assertRaises(SolrResponseError, JSONSolrResponse, raw)

