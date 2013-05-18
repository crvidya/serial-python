""" Testing for the the dtype.py module

The module can be executed on its own or incorporated into a larger test suite.

"""
from StringIO import StringIO

import _path
import _unittest as unittest

from serial.core import DelimitedReader
from serial.core import FixedWidthReader
from serial.core import IntType
from serial.core import ArrayType


# Utility functions.

def accept_filter(record):
    """ A filter function to accept records.

    """
    return record  # accept all records


def reject_filter(record):
    """ A filter function to reject records.

    """
    return record if record["B"] != 3 else None


def modify_filter(record):
    """ A filter function to modify records in place.

    """
    # Input filters can safely modify record.
    record["B"] *= 2 
    return record


def stop_filter(record):
    """ A filter function to stop iteration.

    """
    if record["B"] == 6:
        raise StopIteration
    return record


# Define the TestCase classes for this module. Each public component of the
# module being tested has its own TestCase.

class TabularReaderTest(unittest.TestCase):
    """ Unit testing for TabularReader classes.

    This is an abstract class and should not be called directly by any test
    runners.

    """
    def setUp(self):
        """ Set up the test fixture.

        This is called before each test is run so that they are isolated from
        any side effects. This is part of the unittest API.

        """
        self.data = [
            {"A": [{"x": 1, "y": 2}], "B": 3},
            {"A": [{"x": 4, "y": 5}], "B": 6}]
        return

    def test_fields(self):
        """ Test the fields() method
        
        """
        self.assertSequenceEqual(("A", "B"), self.reader.fields())            

    def test_next(self):
        """ Test the next() method.

        """
        self.assertEqual(self.data[0], self.reader.next())
        return

    def test_iter(self):
        """ Test the __iter__() method.

        """
        self.assertSequenceEqual(self.data, list(self.reader))
        return

    def test_filter_accept(self):
        """ Test a filter that accepts all records.

        """
        self.reader.filter(accept_filter)
        self.assertEqual(self.data[0], self.reader.next())
        return

    def test_filter_reject(self):
        """ Test a filter that rejects a record.

        """
        self.reader.filter(accept_filter)  # test chained filters
        self.reader.filter(reject_filter)
        self.assertEqual(self.data[1], self.reader.next())
        return

    def test_filter_modify(self):
        """ Test a filter that modifies records.

        """
        self.reader.filter(modify_filter)
        self.assertEqual({"A": [{"x": 1, "y": 2}], "B": 6}, self.reader.next())
        return

    def test_filter_stop(self):
        """ Test a filter that stops iteration.

        """
        self.reader.filter(stop_filter)
        self.assertSequenceEqual(self.data[:1], list(self.reader))
        return



class DelimitedReaderTest(TabularReaderTest):
    """ Unit testing for the DelimitedReader class.

    """
    def setUp(self):
        """ Set up the test fixture.

        This is called before each test is run so that they are isolated from
        any side effects. This is part of the unittest API.

        """
        super(DelimitedReaderTest, self).setUp()
        stream = StringIO("1,2,3\n4,5,6\n")
        atype = ArrayType((("x", 0, IntType()), ("y", 1, IntType())))
        fields = (("A", (0, 2), atype), ("B", 2, IntType()))
        self.reader = DelimitedReader(stream, fields, ",")
        return


class FixedWidthReaderTest(TabularReaderTest):
    """ Unit testing for the FixedWidthReader class.

    """
    def setUp(self):
        """ Set up the test fixture.

        This is called before each test is run so that they are isolated from
        any side effects. This is part of the unittest API.

        """
        super(FixedWidthReaderTest, self).setUp()
        stream = StringIO(" 1 2 3\n 4 5 6\n")
        atype = ArrayType((
            ("x", (0, 2), IntType("2d")),
            ("y", (2, 4), IntType("2d"))))
        fields = (("A", (0, 4), atype), ("B", (4, 6), IntType("2d")))
        self.reader = FixedWidthReader(stream, fields)
        return


# Specify the test cases to run for this module (disables automatic discovery).

_TEST_CASES = (DelimitedReaderTest, FixedWidthReaderTest)

def load_tests(loader, tests, pattern):
    """ Define a TestSuite for this module.

    This is part of the unittest API. The last two arguments are ignored. The
    _TEST_CASES global is used to determine which TestCase classes to load
    from this module.

    """
    suite = unittest.TestSuite()
    for test_case in _TEST_CASES:
        tests = loader.loadTestsFromTestCase(test_case)
        suite.addTests(tests)
    return suite


# Make the module executable.

if __name__ == "__main__":
    unittest.main()  # main() calls sys.exit()
