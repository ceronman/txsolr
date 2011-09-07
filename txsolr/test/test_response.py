from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.error import ConnectionDone
from twisted.python.failure import Failure
from twisted.trial.unittest import TestCase
from twisted.web.http import OK
from twisted.web.http_headers import Headers

from txsolr.response import JSONSolrResponse, ResponseConsumer
from txsolr.errors import SolrResponseError


class JSONSorlResponseTest(TestCase):

    def testJsonSolrResponse(self):
        """L{JSONSolrResponse} correctly decodes a Solr JSON Response."""
        raw = """{
                 "responseHeader":{
                  "status":0,
                  "QTime":2,
                  "params":{
                    "indent":"on",
                    "wt":"json",
                    "q":"manuel"}},
                 "response":{"numFound":0,"start":0,"docs":[]}
                 }"""

        r = JSONSolrResponse(raw)

        self.assertEqual(r.header['status'], 0)
        self.assertEqual(r.header['QTime'], 2)
        self.assertEqual(r.results.numFound, 0)
        self.assertEqual(len(r.results.docs), 0)

        raw = """{
                 "responseHeader":{
                  "status":1,
                  "QTime":2,
                  "params":{
                    "indent":"on",
                    "wt":"json",
                    "q":"manuel"}},
                 "response":{"numFound":0,"start":0,"docs":[]}
                 }"""

        self.assertRaises(SolrResponseError, JSONSolrResponse, raw)

        raw = """<response>
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
                </response>"""

        self.assertRaises(SolrResponseError, JSONSolrResponse, raw)

    def testSolrResponseRepr(self):
        """A L{SolrResponse} representation shows the raw response."""
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
        response = JSONSolrResponse(raw)
        self.assertEqual('SolrResponse: %r' % raw, repr(response))


class ResponseConsumerTest(TestCase):

    @inlineCallbacks
    def testResponseConsumerWithGoodResponse(self):
        """
        The L{ResponseConsumer} protocol should fire the given L{Deferred} if
        the given body is a valid Solr response.
        """
        deferred = Deferred()
        consumer = ResponseConsumer(deferred, JSONSolrResponse)
        rawResponse = """{
             "responseHeader":{
              "status":0,
              "QTime":2,
              "params":{
                "indent":"on",
                "wt":"json",
                "q":"manuel"}},
             "response":{"numFound":0,"start":0,"docs":[]}
             }"""
        response = FakeResponse(ConnectionDone, rawResponse)
        response.deliverBody(consumer)
        solrResponse = yield deferred
        self.assertEqual(solrResponse.header['status'], 0)
        self.assertEqual(solrResponse.header['QTime'], 2)
        self.assertEqual(solrResponse.results.numFound, 0)
        self.assertEqual(len(solrResponse.results.docs), 0)

    def testResponseConsumerWithBadResponse(self):
        """
        The L{ResponseConsumer} protocol should fire the given L{Deferred} with
        an C{errback} with L{SolrResponseError} if the decoding fails.
        """
        deferred = Deferred()
        consumer = ResponseConsumer(deferred, JSONSolrResponse)
        response = FakeResponse(ConnectionDone, 'Bad body!')
        response.deliverBody(consumer)
        return self.assertFailure(deferred, SolrResponseError)


class FakeResponse(object):
    """A fake C{Response} that can stream a response payload to a consumer.

    @param reason: An exception instance describing the reason the connection
        was lost.
    @param body: The response payload to deliver to the consumer.
    @param code: Optionally, the HTTP status code for this request.  Default
        is C{200} (OK).
    @param headers: Optionally, a C{Headers} instance with the response
        headers.
    """

    def __init__(self, reason, body, code=None, headers=None):
        self.reason = reason
        self.body = body
        if code:
            self.code = OK
        if headers:
            self.headers = Headers({})

    def deliverBody(self, protocol):
        protocol.dataReceived(self.body)
        protocol.connectionLost(Failure(self.reason))
