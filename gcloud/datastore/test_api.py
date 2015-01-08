# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest2


class Test__require_dataset_id(unittest2.TestCase):

    _MARKER = object()

    def _callFUT(self, passed=_MARKER):
        from gcloud.datastore.api import _require_dataset_id
        if passed is self._MARKER:
            return _require_dataset_id()
        return _require_dataset_id(passed)

    def _monkey(self, dataset_id):
        from gcloud.datastore import _implicit_environ
        from gcloud._testing import _Monkey
        return _Monkey(_implicit_environ, DATASET_ID=dataset_id)

    def test__require_dataset_implicit_unset(self):
        with self._monkey(None):
            with self.assertRaises(EnvironmentError):
                self._callFUT()

    def test__require_dataset_implicit_unset_passed_explicitly(self):
        ID = 'DATASET'
        with self._monkey(None):
            self.assertEqual(self._callFUT(ID), ID)

    def test__require_dataset_id_implicit_set(self):
        IMPLICIT_ID = 'IMPLICIT'
        with self._monkey(IMPLICIT_ID):
            stored_id = self._callFUT()
        self.assertTrue(stored_id is IMPLICIT_ID)

    def test__require_dataset_id_implicit_set_passed_explicitly(self):
        ID = 'DATASET'
        IMPLICIT_ID = 'IMPLICIT'
        with self._monkey(IMPLICIT_ID):
            self.assertEqual(self._callFUT(ID), ID)


class Test_require_connection(unittest2.TestCase):

    _MARKER = object()

    def _callFUT(self, passed=_MARKER):
        from gcloud.datastore.api import _require_connection
        if passed is self._MARKER:
            return _require_connection()
        return _require_connection(passed)

    def _monkey(self, connection):
        from gcloud.datastore import _implicit_environ
        from gcloud._testing import _Monkey
        return _Monkey(_implicit_environ, CONNECTION=connection)

    def test__require_connection_implicit_unset(self):
        with self._monkey(None):
            with self.assertRaises(EnvironmentError):
                self._callFUT()

    def test__require_connection_implicit_unset_passed_explicitly(self):
        CONNECTION = object()
        with self._monkey(None):
            self.assertTrue(self._callFUT(CONNECTION) is CONNECTION)

    def test__require_connection_implicit_set(self):
        IMPLICIT_CONNECTION = object()
        with self._monkey(IMPLICIT_CONNECTION):
            self.assertTrue(self._callFUT() is IMPLICIT_CONNECTION)

    def test__require_connection_implicit_set_passed_explicitly(self):
        IMPLICIT_CONNECTION = object()
        CONNECTION = object()
        with self._monkey(IMPLICIT_CONNECTION):
            self.assertTrue(self._callFUT(CONNECTION) is CONNECTION)


class Test_get_function(unittest2.TestCase):

    def _callFUT(self, keys, missing=None, deferred=None, connection=None):
        from gcloud.datastore.api import get
        return get(keys, missing=missing, deferred=deferred,
                   connection=connection)

    def test_get_no_keys(self):
        results = self._callFUT([])
        self.assertEqual(results, [])

    def test_get_miss(self):
        from gcloud.datastore.key import Key
        from gcloud.datastore.test_connection import _Connection

        DATASET_ID = 'DATASET'
        connection = _Connection()
        key = Key('Kind', 1234, dataset_id=DATASET_ID)
        results = self._callFUT([key], connection=connection)
        self.assertEqual(results, [])

    def test_get_miss_w_missing(self):
        from gcloud.datastore import datastore_v1_pb2 as datastore_pb
        from gcloud.datastore.key import Key
        from gcloud.datastore.test_connection import _Connection

        DATASET_ID = 'DATASET'
        KIND = 'Kind'
        ID = 1234

        # Make a missing entity pb to be returned from mock backend.
        missed = datastore_pb.Entity()
        missed.key.partition_id.dataset_id = DATASET_ID
        path_element = missed.key.path_element.add()
        path_element.kind = KIND
        path_element.id = ID

        # Set missing entity on mock connection.
        connection = _Connection()
        connection._missing = [missed]

        key = Key(KIND, ID, dataset_id=DATASET_ID)
        missing = []
        entities = self._callFUT([key], connection=connection, missing=missing)
        self.assertEqual(entities, [])
        self.assertEqual([missed.key.to_protobuf() for missed in missing],
                         [key.to_protobuf()])

    def test_get_miss_w_deferred(self):
        from gcloud.datastore.key import Key
        from gcloud.datastore.test_connection import _Connection

        DATASET_ID = 'DATASET'
        key = Key('Kind', 1234, dataset_id=DATASET_ID)

        # Set deferred entity on mock connection.
        connection = _Connection()
        connection._deferred = [key.to_protobuf()]

        deferred = []
        entities = self._callFUT([key], connection=connection,
                                 deferred=deferred)
        self.assertEqual(entities, [])
        self.assertEqual([def_key.to_protobuf() for def_key in deferred],
                         [key.to_protobuf()])

    def _make_entity_pb(self, dataset_id, kind, integer_id,
                        name=None, str_val=None):
        from gcloud.datastore import datastore_v1_pb2 as datastore_pb

        entity_pb = datastore_pb.Entity()
        entity_pb.key.partition_id.dataset_id = dataset_id
        path_element = entity_pb.key.path_element.add()
        path_element.kind = kind
        path_element.id = integer_id
        if name is not None and str_val is not None:
            prop = entity_pb.property.add()
            prop.name = name
            prop.value.string_value = str_val

        return entity_pb

    def test_get_hit(self):
        from gcloud.datastore.key import Key
        from gcloud.datastore.test_connection import _Connection

        DATASET_ID = 'DATASET'
        KIND = 'Kind'
        ID = 1234
        PATH = [{'kind': KIND, 'id': ID}]

        # Make a found entity pb to be returned from mock backend.
        entity_pb = self._make_entity_pb(DATASET_ID, KIND, ID,
                                         'foo', 'Foo')

        # Make a connection to return the entity pb.
        connection = _Connection(entity_pb)

        key = Key(KIND, ID, dataset_id=DATASET_ID)
        result, = self._callFUT([key], connection=connection)
        new_key = result.key

        # Check the returned value is as expected.
        self.assertFalse(new_key is key)
        self.assertEqual(new_key.dataset_id, DATASET_ID)
        self.assertEqual(new_key.path, PATH)
        self.assertEqual(list(result), ['foo'])
        self.assertEqual(result['foo'], 'Foo')

    def test_get_hit_multiple_keys_same_dataset(self):
        from gcloud.datastore.key import Key
        from gcloud.datastore.test_connection import _Connection

        DATASET_ID = 'DATASET'
        KIND = 'Kind'
        ID1 = 1234
        ID2 = 2345

        # Make a found entity pb to be returned from mock backend.
        entity_pb1 = self._make_entity_pb(DATASET_ID, KIND, ID1)
        entity_pb2 = self._make_entity_pb(DATASET_ID, KIND, ID2)

        # Make a connection to return the entity pbs.
        connection = _Connection(entity_pb1, entity_pb2)

        key1 = Key(KIND, ID1, dataset_id=DATASET_ID)
        key2 = Key(KIND, ID2, dataset_id=DATASET_ID)
        retrieved1, retrieved2 = self._callFUT(
            [key1, key2], connection=connection)

        # Check values match.
        self.assertEqual(retrieved1.key.path, key1.path)
        self.assertEqual(dict(retrieved1), {})
        self.assertEqual(retrieved2.key.path, key2.path)
        self.assertEqual(dict(retrieved2), {})

    def test_get_hit_multiple_keys_different_dataset(self):
        from gcloud.datastore.key import Key

        DATASET_ID1 = 'DATASET'
        DATASET_ID2 = 'DATASET-ALT'

        # Make sure our IDs are actually different.
        self.assertNotEqual(DATASET_ID1, DATASET_ID2)

        key1 = Key('KIND', 1234, dataset_id=DATASET_ID1)
        key2 = Key('KIND', 1234, dataset_id=DATASET_ID2)
        with self.assertRaises(ValueError):
            self._callFUT([key1, key2], connection=object())

    def test_get_implicit(self):
        from gcloud.datastore import _implicit_environ
        from gcloud.datastore.key import Key
        from gcloud.datastore.test_connection import _Connection
        from gcloud._testing import _Monkey

        DATASET_ID = 'DATASET'
        KIND = 'Kind'
        ID = 1234
        PATH = [{'kind': KIND, 'id': ID}]

        # Make a found entity pb to be returned from mock backend.
        entity_pb = self._make_entity_pb(DATASET_ID, KIND, ID,
                                         'foo', 'Foo')

        # Make a connection to return the entity pb.
        CUSTOM_CONNECTION = _Connection(entity_pb)

        key = Key(KIND, ID, dataset_id=DATASET_ID)
        with _Monkey(_implicit_environ, CONNECTION=CUSTOM_CONNECTION,
                     DATASET_ID=DATASET_ID):
            result, = self._callFUT([key])

        expected_called_with = {
            'dataset_id': DATASET_ID,
            'key_pbs': [key.to_protobuf()],
        }
        self.assertEqual(CUSTOM_CONNECTION._called_with, expected_called_with)

        new_key = result.key
        # Check the returned value is as expected.
        self.assertFalse(new_key is key)
        self.assertEqual(new_key.dataset_id, DATASET_ID)
        self.assertEqual(new_key.path, PATH)
        self.assertEqual(list(result), ['foo'])
        self.assertEqual(result['foo'], 'Foo')


class Test_allocate_ids_function(unittest2.TestCase):

    def _callFUT(self, incomplete_key, num_ids, connection=None):
        from gcloud.datastore.api import allocate_ids
        return allocate_ids(incomplete_key, num_ids, connection=connection)

    def test_allocate_ids(self):
        from gcloud.datastore.key import Key
        from gcloud.datastore.test_connection import _Connection

        DATASET_ID = 'DATASET'
        INCOMPLETE_KEY = Key('KIND', dataset_id=DATASET_ID)
        CONNECTION = _Connection()
        NUM_IDS = 2
        result = self._callFUT(INCOMPLETE_KEY, NUM_IDS, connection=CONNECTION)

        # Check the IDs returned match.
        self.assertEqual([key.id for key in result], range(NUM_IDS))

        # Check connection is called correctly.
        self.assertEqual(CONNECTION._called_dataset_id, DATASET_ID)
        self.assertEqual(len(CONNECTION._called_key_pbs), NUM_IDS)

    def test_allocate_ids_implicit(self):
        from gcloud.datastore import _implicit_environ
        from gcloud.datastore.key import Key
        from gcloud.datastore.test_connection import _Connection
        from gcloud._testing import _Monkey

        CUSTOM_CONNECTION = _Connection()
        NUM_IDS = 2
        with _Monkey(_implicit_environ, CONNECTION=CUSTOM_CONNECTION,
                     DATASET_ID='DATASET'):
            INCOMPLETE_KEY = Key('KIND')
            result = self._callFUT(INCOMPLETE_KEY, NUM_IDS)

        # Check the IDs returned.
        self.assertEqual([key.id for key in result], range(NUM_IDS))

    def test_allocate_ids_with_complete(self):
        from gcloud.datastore import _implicit_environ
        from gcloud.datastore.key import Key
        from gcloud.datastore.test_connection import _Connection
        from gcloud._testing import _Monkey

        CUSTOM_CONNECTION = _Connection()
        with _Monkey(_implicit_environ, CONNECTION=CUSTOM_CONNECTION,
                     DATASET_ID='DATASET'):
            COMPLETE_KEY = Key('KIND', 1234)
            self.assertRaises(ValueError, self._callFUT,
                              COMPLETE_KEY, 2)
