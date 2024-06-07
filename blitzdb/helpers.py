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

def get_value(obj, key, create=False):
    key_fragments = key.split(".")
    current_dict = obj
    last_dict = None
    last_key = None
    for key_fragment in key_fragments:
        try:
            if create and key_fragment not in current_dict:
                current_dict[key_fragment] = {}
        except TypeError:
            if last_dict:
                last_dict[last_key] = {key_fragment: {}}
                current_dict = last_dict[last_key]
            else:
                raise KeyError

        last_key = key_fragment
        last_dict = current_dict
        try:
            current_dict = current_dict[key_fragment]
        except TypeError:
            raise KeyError

    return current_dict


def set_value(obj, key, value, overwrite=True):
    key_fragments = key.split(".")
    current_dict = obj
    last_dict = None
    last_key = None
    for key_fragment in key_fragments:
        try:
            if key_fragment not in current_dict:
                current_dict[key_fragment] = {}
        except TypeError:
            if last_dict:
                last_dict[last_key] = {key_fragment: {}}
                current_dict = last_dict[last_key]
            else:
                raise

        last_dict = current_dict
        last_key = key_fragment
        current_dict = current_dict[key_fragment]

    if (not overwrite) and key_fragments[-1] in last_dict:
        return last_dict[key_fragments[-1]]

    last_dict[key_fragments[-1]] = value
    return last_dict[key_fragments[-1]]


def delete_value(obj, key):
    key_fragments = key.split(".")
    current_dict = obj
    last_dict = None
    for key_fragment in key_fragments:
        try:
            if key_fragment not in current_dict:
                return

        except TypeError:
            return

        last_dict = current_dict
        current_dict = current_dict[key_fragment]

    if key_fragments[-1] in last_dict:
        del last_dict[key_fragments[-1]]
