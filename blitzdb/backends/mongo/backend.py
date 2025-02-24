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


import logging
import traceback
import uuid
from collections import defaultdict
import abc
import copy
from typing import Any, Dict, List, Optional, Union

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.errors import DuplicateKeyError

from blitzdb.backends.base import Backend as BaseBackend
from blitzdb.backends.base import NotInTransaction
from blitzdb.document import Document
from blitzdb.helpers import delete_value, get_value, set_value

from .queryset import QuerySet

logger = logging.getLogger(__name__)


class DotEncoder:

    DOT_MAGIC_VALUE = ":a5b8afc131:"

    @classmethod
    def encode(cls, obj, path):
        def replace_key(key):
            if isinstance(key, str):
                return key.replace(".", cls.DOT_MAGIC_VALUE)

            return key

        if isinstance(obj, dict):
            return {replace_key(key): value for key, value in obj.items()}

        return obj

    @classmethod
    def decode(cls, obj):
        if isinstance(obj, dict):
            return {
                key.replace(cls.DOT_MAGIC_VALUE, "."): value
                for key, value in obj.items()
            }

        return obj


class Backend(BaseBackend):
    """
    A MongoDB backend.

    :param connection: Connection string or MongoDB client instance
    :param database: Name of the database
    :param kwargs: Additional configuration options
    """

    class Meta(BaseBackend.Meta):
        pass

    def __init__(self, connection, database, **kwargs):
        super(Backend, self).__init__(**kwargs)
        
        if isinstance(connection, str):
            self.connection = MongoClient(connection)
        else:
            self.connection = connection
            
        self.database = database
        self.db = self.connection[database]
        self._enable_batch_operations = kwargs.get('enable_batch_operations', False)
        self._batch_size = kwargs.get('batch_size', 1000)
        self._enable_caching = kwargs.get('enable_caching', False)
        
        if self._enable_caching:
            self._cache_manager = self.CacheManager()

    def begin(self):
        raise NotInTransaction("MongoDB does not support transactions")

    def commit(self):
        raise NotInTransaction("MongoDB does not support transactions")

    def rollback(self):
        raise NotInTransaction("MongoDB does not support transactions")

    def create_store(self, store_key):
        return self.db[store_key]

    def get_storage_key_for(self, obj):
        if isinstance(obj, type):
            return self.get_collection_for_cls(obj)
        return self.get_collection_for_cls(obj.__class__)

    def encode_keys(self, obj):
        encoded = {}
        if obj.pk:
            encoded['_id'] = obj.pk
        return encoded

    def decode_keys(self, obj):
        return {'pk': obj.get('_id')}

    def encode_value(self, value):
        if isinstance(value, Document):
            return {'_type': 'object_reference',
                    'collection': self.get_storage_key_for(value),
                    '_id': value.pk}
        return value

    def decode_value(self, value):
        if isinstance(value, dict) and '_type' in value \
                and value['_type'] == 'object_reference':
            cls = self.get_cls_for_collection(value['collection'])
            return self.get(cls, {'pk': value['_id']})
        return value

    # Enhanced save method with batch support
    def save_update(self, obj, store_key):
        if self._enable_batch_operations and isinstance(obj, (list, tuple)):
            return self._batch_save_objects(obj, store_key)
            
        store = self.get_store(store_key)
        if not obj.pk:
            obj.pk = uuid.uuid4().hex
        
        encoded_attrs = self.encode_document(obj)
        encoded_attrs['_id'] = obj.pk
        
        try:
            store.replace_one({'_id': obj.pk}, encoded_attrs, upsert=True)
        except DuplicateKeyError:
            raise obj.DuplicateKeyError(f"Duplicate key error for {obj}")
            
        return {'pk': obj.pk}

    def save_delete(self, obj, store_key):
        store = self.get_store(store_key)
        store.delete_one({'_id': obj.pk})
        return {}

    def _batch_save_objects(self, objects, store_key):
        """Batch save implementation"""
        store = self.get_store(store_key)
        results = []
        
        for i in range(0, len(objects), self._batch_size):
            batch = objects[i:i + self._batch_size]
            operations = []
            
            for obj in batch:
                if not obj.pk:
                    obj.pk = uuid.uuid4().hex
                    
                encoded_attrs = self.encode_document(obj)
                encoded_attrs['_id'] = obj.pk
                
                operations.append(
                    pymongo.ReplaceOne(
                        {'_id': obj.pk},
                        encoded_attrs,
                        upsert=True
                    )
                )
                results.append({'pk': obj.pk})
                
            try:
                store.bulk_write(operations, ordered=False)
            except BulkWriteError as bwe:
                logger.warning(f"Some batch operations failed: {bwe.details}")
                
        return results

    def _batch_delete_objects(self, objects, store_key):
        """Batch delete implementation"""
        store = self.get_store(store_key)
        
        for i in range(0, len(objects), self._batch_size):
            batch = objects[i:i + self._batch_size]
            operations = [
                pymongo.DeleteOne({'_id': obj.pk})
                for obj in batch
            ]
            store.bulk_write(operations, ordered=False)

    class CacheManager:
        """Cache management utility"""
        def __init__(self, max_size=1000):
            self.max_size = max_size
            self._cache = {}

        def get(self, key):
            return self._cache.get(key)

        def set(self, key, value):
            if len(self._cache) >= self.max_size:
                self._cache.pop(next(iter(self._cache)))
            self._cache[key] = value

        def clear(self):
            self._cache.clear()

    def delete_by_primary_keys(self, cls, pks):
        collection = self.get_collection_for_cls(cls)
        if self.autocommit:
            for pk in pks:
                self.db[collection].remove({"_id": pk})
        else:
            self._delete_cache[collection].update({pk: True for pk in pks})

    def delete(self, obj):
        self.call_hook("before_delete", obj)

        collection = self.get_collection_for_cls(obj.__class__)
        if obj.pk == None:
            raise obj.DoesNotExist

        if self.autocommit:
            self.db[collection].remove({"_id": obj.pk})
        else:
            self._delete_cache[collection][obj.pk] = True
            if obj.pk in self._save_cache[collection]:
                del self._save_cache[collection][obj.pk]

    def save_multiple(self, objs):
        if not objs:
            return

        serialized_attributes_list = []
        collection = self.get_collection_for_cls(objs[0].__class__)
        for obj in objs:
            self.call_hook("before_save", obj)
            if obj.pk == None:
                obj.pk = uuid.uuid4().hex
            serialized_attributes = self.serialize(obj.attributes)
            serialized_attributes["_id"] = obj.pk
            serialized_attributes_list.append(serialized_attributes)
        for attributes in serialized_attributes_list:
            if self.autocommit:
                self.db[collection].save(attributes)
            else:
                self._save_cache[collection][attributes["pk"]] = attributes
                if attributes["pk"] in self._delete_cache[collection]:
                    del self._delete_cache[collection][attributes["pk"]]

    def save(self, obj):
        return self.save_multiple([obj])

    def update(self, obj, set_fields=None, unset_fields=None, update_obj=True):

        collection = self.get_collection_for_cls(obj.__class__)

        if obj.pk == None:
            raise obj.DoesNotExist("update() called on document without primary key!")

        def serialize_fields(fields):
            if isinstance(fields, (list, tuple)):
                update_dict = {}
                for key in fields:
                    try:
                        update_dict[key] = get_value(obj, key)
                    except KeyError:
                        pass
            elif isinstance(fields, dict):
                update_dict = fields.copy()
            else:
                raise TypeError("fields must be a list/tuple!")

            return update_dict

        if set_fields:
            set_attributes = serialize_fields(set_fields)
        else:
            set_attributes = {}

        if unset_fields:
            unset_attributes = list(unset_fields)
        else:
            unset_attributes = []

        self.call_hook("before_update", obj, set_attributes, unset_attributes)

        set_attributes = {
            key: self.serialize(value) for key, value in set_attributes.items()
        }

        if update_obj:
            for key, value in set_attributes.items():
                set_value(obj, key, value)
            for key in unset_attributes:
                delete_value(obj, key)

        update_dict = {}

        if set_attributes:
            update_dict["$set"] = set_attributes
        if unset_attributes:
            update_dict["$unset"] = {key: "" for key in unset_attributes}

        if not update_dict:
            return  # nothing to do...

        if self.autocommit:
            self.db[collection].update({"_id": obj.pk}, update_dict)
        else:
            if obj.pk in self._delete_cache[collection]:
                raise obj.DoesNotExist(
                    "update() on document that is marked for deletion!"
                )

            if obj.pk in self._update_cache[collection]:
                update_cache = self._update_cache[collection][obj.pk]
                if set_attributes:
                    if "$set" not in update_cache:
                        update_cache["$set"] = {}
                    for key, value in set_attributes.items():
                        if "$unset" in update_cache and key in update_cache["$unset"]:
                            del update_cache["$unset"][key]
                        update_cache["$set"][key] = value
                if unset_attributes:
                    if "$unset" not in update_cache:
                        update_cache["$unset"] = {}
                    for key in unset_attributes:
                        if "$set" in update_cache and key in update_cache["$set"]:
                            del update_cache["$set"][key]
                        update_cache["$unset"][key] = ""
            else:
                self._update_cache[collection][obj.pk] = update_dict

    def serialize(
        self,
        obj,
        convert_keys_to_str=True,
        embed_level=0,
        encoders=None,
        autosave=True,
        for_query=False,
        path=None,
    ):

        return super().serialize(
            obj,
            convert_keys_to_str=convert_keys_to_str,
            embed_level=embed_level,
            encoders=encoders,
            autosave=autosave,
            path=path,
            for_query=for_query,
        )

    def create_indexes(self, cls_or_collection, params_list):
        for params in params_list:
            self.create_index(cls_or_collection, **params)

    def ensure_indexes(self, include_pk=True):
        for cls in self.classes:
            meta_attributes = self.get_meta_attributes(cls)
            if include_pk:
                self.create_index(cls, fields={"pk": 1}, opts={"unique": True})
            if "indexes" in meta_attributes:
                self.create_indexes(cls, meta_attributes["indexes"])

    def create_index(self, cls_or_collection, *args, **kwargs):
        if not isinstance(cls_or_collection, str):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection

        if "fields" not in kwargs:
            raise AttributeError(
                "You must specify the 'fields' parameter when creating an index!"
            )

        if "opts" in kwargs:
            opts = kwargs["opts"]
        else:
            opts = {}
        try:
            self.db[collection].ensure_index(list(kwargs["fields"].items()), **opts)
        except pymongo.errors.OperationFailure as failure:
            traceback.print_exc()
            # The index already exists with different options, so we drop it and recreate it...
            self.db[collection].drop_index(list(kwargs["fields"].items()))
            self.db[collection].ensure_index(list(kwargs["fields"].items()), **opts)

    def _canonicalize_query(self, query):

        """Transform the query dictionary to replace e.g. documents with
        __ref__ fields."""

        def transform_query(q):

            for encoder in self.query_encoders:
                q = encoder.encode(q, [])

            if isinstance(q, dict):
                nq = {}
                for key, value in q.items():
                    new_key = key
                    if (
                        isinstance(value, dict)
                        and len(value) == 1
                        and list(value.keys())[0].startswith("$")
                    ):
                        if list(value.keys())[0] in ("$all", "$in"):
                            if list(value.values())[0] and isinstance(
                                list(value.values())[0][0], Document
                            ):
                                if self._use_pk_based_refs:
                                    new_key += ".pk"
                                else:
                                    new_key += ".__ref__"
                    elif isinstance(value, Document):
                        if self._use_pk_based_refs:
                            new_key += ".pk"
                        else:
                            new_key += ".__ref__"
                    nq[new_key] = transform_query(value)
                return nq

            elif isinstance(q, (list, QuerySet, tuple)):
                return [transform_query(x) for x in q]

            elif isinstance(q, Document):
                collection = self.get_collection_for_obj(q)
                if self._use_pk_based_refs:
                    return q.pk

                else:
                    return f"{collection}:{q.pk}"

            else:
                return q

        return transform_query(query)

    def get(self, cls_or_collection, properties, raw=False, only=None):
        if not isinstance(cls_or_collection, str):
            collection = self.get_collection_for_cls(cls_or_collection)
        else:
            collection = cls_or_collection
        cls = self.get_cls_for_collection(collection)
        queryset = self.filter(cls_or_collection, properties, raw=raw, only=only)
        if len(queryset) == 0:
            raise cls.DoesNotExist

        elif len(queryset) > 1:
            raise cls.MultipleDocumentsReturned

        return queryset[0]

    def filter(self, cls_or_collection, query, raw=False, only=None):
        """Filter objects from the database that correspond to a given set of
        properties.

        See :py:meth:`blitzdb.backends.base.Backend.filter` for documentation of individual parameters

        .. note::

            This function supports most query operators that are available in MongoDB and returns
            a query set that is based on a MongoDB cursor.
        """

        if not isinstance(cls_or_collection, str):
            collection = self.get_collection_for_cls(cls_or_collection)
            cls = cls_or_collection
        else:
            collection = cls_or_collection
            cls = self.get_cls_for_collection(collection)

        canonical_query = self._canonicalize_query(query)

        args = {}

        if only:
            if isinstance(only, tuple):
                args["projection"] = list(only)
            else:
                args["projection"] = only

        return QuerySet(
            self,
            cls,
            self.db[collection].find(canonical_query, **args),
            raw=raw,
            only=only,
        )
