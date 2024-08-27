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

"""
Serializers take a Python object and return a string representation of it.
BlitzDB currently supports several differen JSON serializers,
as well as a cPickle serializer.
"""

import json
import pickle as cPickle

from .utils import JsonEncoder


class JsonSerializer:
    @classmethod
    def serialize(cls, data):
        if isinstance(data, bytes):
            return json.dumps(
                data.decode("utf-8"), cls=JsonEncoder, ensure_ascii=False
            ).encode("utf-8")

        else:
            return json.dumps(data, cls=JsonEncoder, ensure_ascii=False).encode("utf-8")

    @classmethod
    def deserialize(cls, data):
        return json.loads(data.decode("utf-8"))


class PickleSerializer:
    @classmethod
    def serialize(cls, data):
        return cPickle.dumps(data, cPickle.HIGHEST_PROTOCOL)

    @classmethod
    def deserialize(cls, data):
        return cPickle.loads(data)


try:
    import cjson

    class CJsonSerializer:
        @classmethod
        def serialize(cls, data):
            return cjson.encode(data)

        @classmethod
        def deserialize(cls, data):
            return cjson.decode(data)


except ImportError:
    pass
