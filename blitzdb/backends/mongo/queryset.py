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

from blitzdb.queryset import QuerySet as BaseQuerySet


class QuerySet(BaseQuerySet):
    def __init__(self, backend, cls, cursor, raw=False, only=None):
        super().__init__(backend, cls)
        self._cursor = cursor
        self._raw = raw
        self._only = only

    def __iter__(self):
        return self

    def __len__(self):
        return self._cursor.count()

    def _create_object_for(self, json_attributes):
        if self._raw:
            return json_attributes

        deserialized_attributes = self.backend.deserialize(
            json_attributes, create_instance=False
        )
        if "_id" in deserialized_attributes:
            del deserialized_attributes["_id"]
        return self.backend.create_instance(self.cls, deserialized_attributes)

    def as_list(self):
        return [self._create_object_for(json) for json in list(self._cursor)]

    def next(self):
        json_attributes = next(self._cursor)
        obj = self._create_object_for(json_attributes)
        return obj

    __next__ = next

    def __getitem__(self, key):
        if isinstance(key, slice):
            start, stop, step = key.start, key.stop, key.step
            if step != None:
                raise IndexError("MongoDB slices do not support slice steps")

            if key.start == None:
                start = 0
            if key.stop == None:
                stop = self._cursor.count()
            if start < 0:
                start = self._cursor.count() + start
            if stop < 0:
                stop = self._cursor.count() + stop
            key = slice(start, stop)
            return self.__class__(
                self.backend, self.cls, self._cursor.__getitem__(key), raw=self._raw
            )

        if key < 0:
            key = self._cursor.count() + key
        json_attributes = self._cursor[key]
        obj = self._create_object_for(json_attributes)
        return obj

    def __contains__(self, obj):
        pks = self._cursor.distinct("_id")
        if isinstance(obj, list) or isinstance(obj, tuple):
            obj_list = obj
        else:
            obj_list = [obj]
        for obj in obj_list:
            if obj.pk not in pks:
                return False

        return True

    def rewind(self):
        self._cursor.rewind()

    def delete(self):
        self.backend.delete_by_primary_keys(self.cls, self._cursor.distinct("_id"))

    def sort(self, *args, **kwargs):
        self._cursor.sort(*args, **kwargs)
        return self

    def limit(self, *args, **kwargs):
        self._cursor.limit(*args, **kwargs)
        return self

    def filter(self, *args, **kwargs):
        return self.backend.filter(self.cls, *args, initial_keys=self.keys, **kwargs)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        if isinstance(other, QuerySet):
            if self.cls == other.cls and set(self._cursor.distinct("_id")) == set(
                other._cursor.distinct("_id")
            ):
                return True

        elif isinstance(other, list):
            if len(other) != len(self.keys):
                return False

            objs = list(self)
            if other == objs:
                return True

        return False
