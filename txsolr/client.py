"""
Solr Client for Twisted
"""

import urlparse

from twisted.internet import reactor, defer
from twisted.web.client import Agent
from twisted.web.http_headers import Headers

from txsolr.input import XMLInput


class SolrClient(object):
    """
    A Solr Client. Used to make requests to a Solr Server
    """

    def __init__(self, url, inputCreator=XMLInput):
        """
        Creates the Solr Client object

        @param url: the url of the Solr server
        @param inputCreator: a class that is going to produce the input for the
                             server, by default uses a simple input creator
        """

        self.url = url.rstrip('/')
        self.inputCreator = inputCreator
        self._agent = Agent(reactor)

    def _request(self, method, path, headers, bodyProducer):
        url = urlparse.urljoin(self.url, path)
        headers.update({'User-Agent': ['txSolr']})
        headers = Headers(headers)
        return self._agent.request(method, url, headers, bodyProducer)



