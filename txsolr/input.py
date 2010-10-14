# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Encoders and decoders for Solr requests and responses
"""

from xml.etree import cElementTree as ElementTree
from datetime import date, datetime

from zope.interface import implements
from twisted.internet import defer
from twisted.web.iweb import IBodyProducer

from txsolr.errors import InputError

__all__ = ['StringProducer', 'SimpleXMLInputFactory', 'escapeTerm']


def escapeTerm(term):
    """
    Escapes special characters of the Lucene Query Syntax.

    @param term: The term to be escaped.
    @return the term with all the special characters escaped.
    """
    specialChars = set(r'\ + - & | ! ( ) { } [ ] ^ " ~ * ? :'.split())
    return ''.join(('\\' + c if c in specialChars else c) for c in term)


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

        # TODO: Document exceptions
        try:
            return unicode(value)
        except UnicodeError:
            raise InputError('Unable to decode value %r' % value)

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
            idElement.text = self._encodeValue(id)
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

    def createCommit(self, waitFlush=None,
                           waitSearcher=None,
                           expungeDeletes=None):

        commitElement = ElementTree.Element('commit')

        if waitFlush is not None:
            waitFlush = 'true' if waitFlush else 'false'
            commitElement.set('waitFlush', waitFlush)

        if waitSearcher is not None:
            waitSearcher = 'true' if waitSearcher else 'false'
            commitElement.set('waitSearcher', waitSearcher)

        if expungeDeletes is not None:
            expungeDeletes = 'true' if expungeDeletes else 'false'
            commitElement.set('expungeDeletes', expungeDeletes)

        result = ElementTree.tostring(commitElement)
        return StringProducer(result)

    def createRollback(self):
        rollbackElement = ElementTree.Element('rollback')
        result = ElementTree.tostring(rollbackElement)
        return StringProducer(result)

    def createOptimize(self, waitFlush=None,
                             waitSearcher=None,
                             maxSegments=None):

        optimizeElement = ElementTree.Element('optimize')

        if waitFlush is not None:
            waitFlush = 'true' if waitFlush else 'false'
            optimizeElement.set('waitFlush', waitFlush)

        if waitSearcher is not None:
            waitSearcher = 'true' if waitSearcher else 'false'
            optimizeElement.set('waitSearcher', waitSearcher)

        if maxSegments is not None:
            maxSegments = str(maxSegments)
            optimizeElement.set('maxSegments', maxSegments)

        result = ElementTree.tostring(optimizeElement)
        return StringProducer(result)
