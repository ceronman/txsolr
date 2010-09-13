import unittest
from datetime import datetime, date

from txsolr.input import SimpleXMLInputFactory

class XMLInputTest(unittest.TestCase):

    def setUp(self):
        self.input = SimpleXMLInputFactory()

    def test_encodeValue(self):
        """
        Tests the Python value to XML value encoder
        """

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

    def test_createAdd(self):
        """
        Tests the creation of add input for the request
        """

        document = {'id': 1, 'text': 'hello'}
        expected = ('<add><doc><field name="text">hello</field>'
                    '<field name="id">1</field></doc></add>')
        input = self.input.createAdd(document).body
        self.assertEqual(input, expected, 'Wrong input')

    def test_createAddWithCollection(self):

        document = {'id': 1, 'collection': [1, 2, 3]}
        expected = ('<add><doc><field name="id">1</field>'
                    '<field name="collection">1</field>'
                    '<field name="collection">2</field>'
                    '<field name="collection">3</field></doc></add>')

        input = self.input.createAdd(document).body
        self.assertEqual(input, expected, 'Wrong input')

    def test_createAddExceptions(self):

        self.assertRaises(AttributeError, self.input.createAdd, None)
        self.assertRaises(AttributeError, self.input.createAdd, 'string')


    def test_createAddWithOverwrite(self):
        document = {'id': 1, 'text': 'hello'}
        expected = ('<add overwrite="true">'
                    '<doc><field name="text">hello</field>'
                    '<field name="id">1</field></doc></add>')

        input = self.input.createAdd(document, overwrite=True).body
        self.assertEqual(input, expected, 'Wrong input')

    def test_createAddWithCommitWithin(self):
        document = {'id': 1, 'text': 'hello'}
        expected = ('<add commitWithin="80">'
                    '<doc><field name="text">hello</field>'
                    '<field name="id">1</field></doc></add>')

        input = self.input.createAdd(document, commitWithin=80).body
        self.assertEqual(input, expected, 'Wrong input')

    def test_createDelete(self):
        """
        Tests the creation fo delete input for the request
        """

        id = 123
        expected = '<delete><id>123</id></delete>'
        self.assertEqual(self.input.createDelete(id).body, expected)

        id = '<hola>'
        expected = '<delete><id>&lt;hola&gt;</id></delete>'
        self.assertEqual(self.input.createDelete(id).body, expected)

        id = [1, 2, 3]
        expected = '<delete><id>1</id><id>2</id><id>3</id></delete>'
        self.assertEqual(self.input.createDelete(id).body, expected)




