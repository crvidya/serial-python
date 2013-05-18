""" Writer types.

Writers convert data records to lines of text.

"""
from ._util import make_fields

__all__ = ("DelimitedWriter", "FixedWidthWriter")


class _Writer(object):
    """ Abstract base class for all serial data writers.

    Serial data consists of individual records stored as lines of text.

    """
    def __init__(self, stream):
        """ Initialize this object.

        The output stream is any object that implements write() to write a line
        of text.

        """
        self._stream = stream
        self._filters = []
        return

    def filter(self, callback=None):
        """ Add a filter to this writer or clear all filters.

        A filter is a callable object that accepts a data record as its only
        argument. Based on this record the filter can perform the following
        actions:
        1. Return None to reject the record (it will not be written).
        2. Return the data record as is.
        3. Return a *new record.
        4. Raise StopIteration to signal the end of input.
        
        *Take care not to modify the argument unless the caller doesn't
         expect write() to be free of side effects.

        """
        if callback is None:
            self._filters = []
        else:
            self._filters.append(callback)
        return

    def write(self, record):
        """ Write a record to the output stream.
        
        """
        for callback in self._filters:
            record = callback(record)
            if record is None:
                return
        self._put(record)
        return

    def dump(self, records):
        """ Write all records to the output stream.
        
        """
        for record in records:
            self.write(record)
        return

    def _put(self, record):
        """ Write a record to the output stream.
        
        This is called after the record has been passed through all filters.
        
        """
        raise NotImplementedError


class _TabularWriter(_Writer):
    """ Abstract base class for tabular data writers.

    Tabular data is organized fields such that each field occupies the same
    position in each record. One line of text corresponds to a one complete
    record.

    """
    def __init__(self, stream, fields, endl="\n"):
        """ Initialize this object.

        """
        super(_TabularWriter, self).__init__(stream)
        self._fields = make_fields(fields)
        self._endl = endl
        return

    def fields(self):
        """ Return the field names defined for this writer.
        
        """
        return tuple((field.name for field in self._fields))
        
    def _put(self, record):
        """ Write a filtered record to the output stream.

        """
        tokens = [field.dtype.encode(record.get(field.name)) for field in
                  self._fields]
        self._stream.write(self._merge(tokens) + self._endl)
        return

    def _merge(self, tokens):
        """ Create a line of text from a sequence of tokens.

        """
        raise NotImplementedError


class DelimitedWriter(_TabularWriter):
    """ A writer for fields delineated by a delimiter.

    The position of each scalar field is be given as an integer index, and the
    position of an array field is the pair [beg, end).

    """
    def __init__(self, stream, fields, delim=" ", endl="\n"):
        """ Initialize this object.

        At this time there is no escaping of characters in the input records
        that match the delimiter; this may cause issues when trying to parse
        the resulting output.

        """
        super(DelimitedWriter, self).__init__(stream, fields, endl)
        self._delim = delim
        return

    def _merge(self, tokens):
        """ Create a line of text from a sequence of tokens.

        """
        pos = 0
        while pos < len(tokens):
            # A token can itself be a sequence of tokens (c.f. ArrayType).
            token = tokens[pos]
            if isinstance(token, basestring):
                pos += 1
            else:
                # A sequence of tokens; expand inline.
                tokens[pos:pos+1] = token
                pos += len(token)
        return self._delim.join(tokens)


class FixedWidthWriter(_TabularWriter):
    """ A writer for fields delineated by character position.

    The character position of each field is given as the pair [beg, end).

    """
    def _merge(self, tokens):
        """ Create a line of text from a sequence of tokens.

        """
        # The character positions in self.fields don't matter; tokens must be
        # in the correct order, and each token must be the correct width for
        # that field. The DataType format for a fixed-width field *MUST* have
        # a field width, e.g. '8s'.
        pos = 0
        while pos < len(tokens):
            # A token can itself be a sequence of tokens (c.f. ArrayType).
            token = tokens[pos]
            if isinstance(token, basestring):
                pos += 1
            else:
                # A sequence of tokens; expand inline.
                tokens[pos:pos+1] = token
                pos += len(token)
        return "".join(tokens)
