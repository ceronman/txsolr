import unittest
from datetime import datetime, date

from txsolr.input import SimpleXMLInputFactory, escapeTerm


class EscapingTest(unittest.TestCase):

    def testEscapeTerm(self):
        terms = [(r'Hello*World', r'Hello\*World'),
                 (r'Hello "World"', r'Hello \"World\"'),
                 (r'Hello |&^"~*?', r'Hello \|\&\^\"\~\*\?'),
                 (r'Hello (World)', r'Hello \(World\)'),
                 (r'Hello World', r'Hello World'), ]

        for raw, escaped in terms:
            self.assertEqual(escapeTerm(raw), escaped)


class SimpleXMLInputFactoryTest(unittest.TestCase):

    def setUp(self):
        self.input = SimpleXMLInputFactory()

    # TODO: Don't use private methods for this test.
    def testEncodeValue(self):
        """L{SimpleXMLInputFactory} encodes primitive values correctly."""
        value = datetime(2010, 1, 1, 0, 0, 0)
        value = self.input._encodeValue(value)
        self.assertEqual(value, '2010-01-01T00:00:00Z')

        value = date(2010, 1, 1)
        value = self.input._encodeValue(value)
        self.assertEqual(value, '2010-01-01T00:00:00Z')

        value = True
        value = self.input._encodeValue(value)
        self.assertEqual(value, 'true')

        value = 'sample str'
        value = self.input._encodeValue(value)
        self.assert_(isinstance(value, unicode))

        value = None
        value = self.input._encodeValue(value)
        self.assert_(isinstance(value, unicode))

    def testCreateAdd(self):
        """
        L{SimpleXMLInputFactory.createAdd} creates a correct body for an C{add}
        request.
        """
        document = {'id': 1, 'text': 'hello'}
        expected = ('<add><doc><field name="text">hello</field>'
                    '<field name="id">1</field></doc></add>')
        input = self.input.createAdd(document).body
        self.assertEqual(input, expected, 'Wrong input')

    def testCreateAddWithCollection(self):
        """
        L{SimpleXMLInputFactory.createAdd} creates a correct body for an C{add}
        request with a sequence field.
        """
        document = {'id': 1, 'collection': [1, 2, 3]}
        expected = ('<add><doc><field name="id">1</field>'
                    '<field name="collection">1</field>'
                    '<field name="collection">2</field>'
                    '<field name="collection">3</field></doc></add>')

        input = self.input.createAdd(document).body
        self.assertEqual(input, expected, 'Wrong input')

    def testCreateAddWithWrongValues(self):
        """
        L{SimpleXMLInputFactory.createAdd} raises C{AttributeError} if one of
        the given values is not a proper C{dict} document.
        """
        self.assertRaises(AttributeError, self.input.createAdd, None)
        self.assertRaises(AttributeError, self.input.createAdd, 'string')

    def testCreateAddWithOverwrite(self):
        """
        L{SimpleXMLInputFactory.createAdd} can add an C{overwrite} option.
        """
        document = {'id': 1, 'text': 'hello'}
        expected = ('<add overwrite="true">'
                    '<doc><field name="text">hello</field>'
                    '<field name="id">1</field></doc></add>')

        input = self.input.createAdd(document, overwrite=True).body
        self.assertEqual(input, expected, 'Wrong input')

    def testCreateAddWithCommitWithin(self):
        """
        L{SimpleXMLInputFactory.createAdd} can add a C{commitWithin} option.
        """
        document = {'id': 1, 'text': 'hello'}
        expected = ('<add commitWithin="80">'
                    '<doc><field name="text">hello</field>'
                    '<field name="id">1</field></doc></add>')

        input = self.input.createAdd(document, commitWithin=80).body
        self.assertEqual(input, expected, 'Wrong input')

    def testCreateDelete(self):
        """
        L{SimpleXMLInputFactory.createDelete} creates a correct body for a
        C{delete} request.
        """
        id = 123
        expected = '<delete><id>123</id></delete>'
        self.assertEqual(self.input.createDelete(id).body, expected)

    def testCreateDeleteWithEncoding(self):
        """
        L{SimpleXMLInputFactory.createDelete} correctly escapes XML special
        characters.
        """
        id = '<hola>'
        expected = '<delete><id>&lt;hola&gt;</id></delete>'
        self.assertEqual(self.input.createDelete(id).body, expected)

    def testCreateDeleteMany(self):
        """
        L{SimpleXMLInputFactory.createDelete} correctly handles multiple
        document IDs.
        """
        id = [1, 2, 3]
        expected = '<delete><id>1</id><id>2</id><id>3</id></delete>'
        self.assertEqual(self.input.createDelete(id).body, expected)

    def testCommit(self):
        """
        L{SimpleXMLInputFactory.createCommit} creates a correct body for a
        C{commit} request.
        """
        input = self.input.createCommit().body
        expected = '<commit />'
        self.assertEqual(input, expected)

    def testCommitWaitFlush(self):
        """
        L{SimpleXMLInputFactory.createCommit} can add a C{waitFlush} parameter.
        """
        input = self.input.createCommit(waitFlush=True).body
        expected = '<commit waitFlush="true" />'
        self.assertEqual(input, expected)

    def testCommitWaitSearcher(self):
        """
        L{SimpleXMLInputFactory.createCommit} can add a C{waitSearcher}
        parameter.
        """
        input = self.input.createCommit(waitSearcher=True).body
        expected = '<commit waitSearcher="true" />'
        self.assertEqual(input, expected)

    def testCommitExpungeDeletes(self):
        """
        L{SimpleXMLInputFactory.createCommit} can add an C{expungeDeletes}
        parameter.
        """
        input = self.input.createCommit(expungeDeletes=True).body
        expected = '<commit expungeDeletes="true" />'
        self.assertEqual(input, expected)

    def testOptimize(self):
        """
        L{SimpleXMLInputFactory.createOptimize} creates a correct body for an
        C{optimize} request.
        """
        input = self.input.createOptimize().body
        expected = '<optimize />'
        self.assertEqual(input, expected)

    def testOptimizeWaitFlush(self):
        """
        L{SimpleXMLInputFactory.createOptimize} can add a C{waitFlush}
        parameter.
        """
        input = self.input.createOptimize(waitFlush=True).body
        expected = '<optimize waitFlush="true" />'
        self.assertEqual(input, expected)

    def testOptimizeWaitSearcher(self):
        """
        L{SimpleXMLInputFactory.createOptimize} can add a C{waitSearcher}
        parameter.
        """
        input = self.input.createOptimize(waitSearcher=True).body
        expected = '<optimize waitSearcher="true" />'
        self.assertEqual(input, expected)

    def testOptimizeMaxSegments(self):
        """
        L{SimpleXMLInputFactory.createCommit} can add a C{maxSegments}
        parameter.
        """
        input = self.input.createOptimize(maxSegments=2).body
        expected = '<optimize maxSegments="2" />'
        self.assertEqual(input, expected)
