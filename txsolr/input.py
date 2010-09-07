"""
Encoders and decoders for Solr requests and responses
"""

from xml.etree import cElementTree as ElementTree
from datetime import date, datetime

from zope.interface import implements
from twisted.internet import defer
from twisted.web.iweb import IBodyProducer


class StringProducer(object):
    """
    Very basic producer used for Agent requests
    """

    implements(IBodyProducer)

    def __init__(self, body):
        self.body = str(body)
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return defer.succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class SimpleXMLInputFactory(object):
    """
    Creates XML input messages for Solr
    """

    def __init__(self):
        self.contentType = 'text/xml'

    def _encodeValue(self, value):
        if isinstance(value, datetime):
            value = value.strftime('%Y-%m-%dT%H:%M:%SZ')

        elif isinstance(value, date):
            value = value.strftime('%Y-%m-%dT00:00:00Z')

        elif isinstance(value, bool):
            value = 'true' if value else 'false'

        return unicode(value)

    def _decodeValue(self, value):
        pass

    def createAdd(self, documents):
        """
        Create an add request in XML format
        """

        addElement = ElementTree.Element('add')
        for document in documents:

            docElement = ElementTree.Element('doc')
            for key, value in document.iteritems():

                if isinstance(value, (tuple, list, set)):
                    values = value
                else:
                    values = [value]

                for v in values:
                    if v is None:
                        continue

                    fieldElement = ElementTree.Element('field', name=key)
                    fieldElement.text = self._encodeValue(v)
                    docElement.append(fieldElement)
            addElement.append(docElement)

        result = ElementTree.tostring(addElement)
        return StringProducer(result)

    def createDelete(self, id):
        deleteElement = ElementTree.Element('delete')
        if isinstance(id, (tuple, list, set)):
            ids = id
        else:
            ids = [id]

        for id in ids:
            idElement = ElementTree.Element('id')
            idElement.text = unicode(id)
            deleteElement.append(idElement)

        result = ElementTree.tostring(deleteElement)
        return StringProducer(result)

    def createDeleteQuery(self):
        """Missing"""
        pass

    def createCommit(self):
        commitElement = ElementTree.Element('commit')
        result = ElementTree.tostring(commitElement)
        return StringProducer(result)

    def createOptimize(self):
        """Missing"""
        pass

    def createRollback(self):
        """Missing"""
        pass

