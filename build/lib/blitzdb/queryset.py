# The MIT License (MIT)
# 
# Copyright (c) 2014 Andreas Dewes
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import abc


class QuerySet:

    """Stores information about objects returned by a database query and
    retrieves instances of these objects if necessary.

    :param backend: The backend to use for :py:meth:filter`filtering` etc.
    :param cls: The class of the documents stored in the query set.
    """

    __metaclass__ = abc.ABCMeta

    ASCENDING = 1
    DESCENDING = -1

    def __init__(self, backend, cls):
        """Initializes a query set."""
        self.cls = cls
        self.backend = backend

    @abc.abstractmethod
    def __getitem__(self, i):
        """Returns a specific element from a query set.

        If i is a slice instead of an index (e.g. qs[:50]), returns a
        subset of the query results. This allows user to specify an
        offset and/or limit for the query.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self):
        """Deletes all objects contained in this query set from the
        database."""
        raise NotImplementedError

    @abc.abstractmethod
    def sort(self, *args, **kwargs):
        """Sort documents in this query set based on a key and order.

        :param key: the property name to sort on
        :param order: either `blitzdb.queryset.QuerySet.ASCENDING`
                      or `blitzdb.queryset.QuerySet.DESCENDING`
        :returns: this queryset
        """

    @abc.abstractmethod
    def filter(self, *args, **kwargs):
        """Performs a `filter` operation on all documents contained in the
        query set.

        See :py:meth:`blitzdb.backends.base.Backend.filter` for more
        details.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __len__(self):
        """Return the number of documents contained in this query set."""
        raise NotImplementedError

    @abc.abstractmethod
    def __ne__(self, other):
        """Checks if two query sets are unequal.

        :param other: The object this query set is compared to.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __eq__(self, other):
        """Checks if two query sets are equal. Implement this in your derived
        query set class.

        :param other: The object this query set is compared to.
        """
        raise NotImplementedError
