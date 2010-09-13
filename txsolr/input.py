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

    # TODO: Remove me
    def _decodeValue(self, value):
        pass

    def createAdd(self, document, overwrite=None, commitWithin=None):
        """
        Create an add request in XML format
        """

        if isinstance(document, (tuple, list, set)):
            documents = document
        else:
            documents = [document]

        addElement = ElementTree.Element('add')

        if overwrite is not None:
            overwrite = 'true' if overwrite else 'false'
            addElement.set('overwrite', overwrite)

        if commitWithin is not None:
            commitWithin = str(commitWithin)
            addElement.set('commitWithin', commitWithin)

        for doc in documents:

            docElement = ElementTree.Element('doc')
            for key, value in doc.iteritems():

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
        if isinstance(id, (tuple, list, set)):
            ids = id
        else:
            ids = [id]

        deleteElement = ElementTree.Element('delete')

        for id in ids:
            idElement = ElementTree.Element('id')
            idElement.text = unicode(id)
            deleteElement.append(idElement)

        result = ElementTree.tostring(deleteElement)
        return StringProducer(result)

    def createDeleteByQuery(self, query):
        deleteElement = ElementTree.Element('delete')
        queryElement = ElementTree.Element('query')
        queryElement.text = query
        deleteElement.append(queryElement)

        result = ElementTree.tostring(deleteElement)
        return StringProducer(result)

    def createCommit(self):
        commitElement = ElementTree.Element('commit')
        result = ElementTree.tostring(commitElement)
        return StringProducer(result)

    def createRollback(self):
        rollbackElement = ElementTree.Element('rollback')
        result = ElementTree.tostring(rollbackElement)
        return StringProducer(result)

    def createOptimize(self):
        optimizeElement = ElementTree.Element('optimize')
        result = ElementTree.tostring(optimizeElement)
        return StringProducer(result)
