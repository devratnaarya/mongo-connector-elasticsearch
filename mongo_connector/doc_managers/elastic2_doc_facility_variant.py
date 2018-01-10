# Copyright 2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Elasticsearch implementation of the DocManager interface.

Receives documents from an OplogThread and takes the appropriate actions on
Elasticsearch.
"""
import base64
import logging
import warnings
import time

from threading import Timer

import bson.json_util

from elasticsearch import Elasticsearch, exceptions as es_exceptions, connection as es_connection
from elasticsearch.helpers import bulk, scan, streaming_bulk, BulkIndexError

from mongo_connector import errors
from mongo_connector.compat import u
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK)
from mongo_connector.util import exception_wrapper, retry_until_ok
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter

from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
m_product = client.catalog.product
m_facility_variant = client.catalog.facility_variant

_HAS_AWS = True
try:
    from boto3 import session
    from requests_aws_sign import AWSV4Sign
except ImportError:
    _HAS_AWS = False

wrap_exceptions = exception_wrapper({
    BulkIndexError: errors.OperationFailed,
    es_exceptions.ConnectionError: errors.ConnectionFailed,
    es_exceptions.TransportError: errors.OperationFailed,
    es_exceptions.NotFoundError: errors.OperationFailed,
    es_exceptions.RequestError: errors.OperationFailed})

LOG = logging.getLogger(__name__)

DEFAULT_AWS_REGION = 'us-east-1'


def convert_aws_args(aws_args):
    """Convert old style options into arguments to boto3.session.Session."""
    if not isinstance(aws_args, dict):
        raise errors.InvalidConfiguration(
            'Elastic DocManager config option "aws" must be a dict')
    old_session_kwargs = dict(region='region_name',
                              access_id='aws_access_key_id',
                              secret_key='aws_secret_access_key')
    new_kwargs = {}
    for arg in aws_args:
        if arg in old_session_kwargs:
            new_kwargs[old_session_kwargs[arg]] = aws_args[arg]
        else:
            new_kwargs[arg] = aws_args[arg]
    return new_kwargs


def create_aws_auth(aws_args):
    try:
        aws_session = session.Session(**convert_aws_args(aws_args))
    except TypeError as exc:
        raise errors.InvalidConfiguration(
            'Elastic DocManager unknown aws config option: %s' % (exc,))
    return AWSV4Sign(aws_session.get_credentials(),
                     aws_session.region_name or DEFAULT_AWS_REGION,
                     'es')


class DocManager(DocManagerBase):
    """Elasticsearch implementation of the DocManager interface.

    Receives documents from an OplogThread and takes the appropriate actions on
    Elasticsearch.
    """

    def __init__(self, url, auto_commit_interval=DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id', chunk_size=DEFAULT_MAX_BULK,
                 meta_index_name="mongodb_meta", meta_type="mongodb_meta",
                 attachment_field="content", **kwargs):
        client_options = kwargs.get('clientOptions', {})
        client_options.setdefault('sniff_on_start', True)
        client_options.setdefault('sniff_on_connection_fail', True)
        client_options.setdefault('sniffer_timeout', 60)
        if 'aws' in kwargs:
            if not _HAS_AWS:
                raise errors.InvalidConfiguration(
                    'aws extras must be installed to sign Elasticsearch '
                    'requests. Install with: '
                    'pip install elastic2-doc-manager[aws]')
            client_options['http_auth'] = create_aws_auth(kwargs['aws'])
            client_options['use_ssl'] = True
            client_options['verify_certs'] = True
            client_options['connection_class'] = \
                es_connection.RequestsHttpConnection
        if type(url) is not list:
            url = [url]
        self.elastic = Elasticsearch(hosts=url, **client_options)
        self.auto_commit_interval = auto_commit_interval
        self.meta_index_name = meta_index_name
        self.meta_type = meta_type
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        self.routing = kwargs.get('routing', {})
        if self.auto_commit_interval not in [None, 0]:
            self.run_auto_commit()
        self._formatter = DefaultDocumentFormatter()

        self.has_attachment_mapping = False
        self.attachment_field = attachment_field

    def _index_and_mapping(self, namespace):
        """Helper method for getting the index and type from a namespace."""
        index, doc_type = namespace.split('.', 1)
        return index.lower(), doc_type

    def _get_parent_field(self, index, doc_type):
        """Get the parent field name for this collection."""
        try:
            return self.routing[index][doc_type]['variant_id']
        except KeyError:
            return None

    def _is_child_type(self, index, doc_type):
        """Return True if this mapping type is a child"""
        return self._get_parent_field(index, doc_type) is not None

    def _get_parent_id_from_mongodb(self, index, doc_type, doc):
        """Get parent ID from doc"""
        parent_field = self._get_parent_field(index, doc_type)
        if parent_field is None:
            return None

        return self._formatter.transform_value(doc.pop(parent_field, None))

    def _get_parent_id_from_elastic(self, doc):
        """Get parent ID from doc"""
        return doc.get('_parent')

    def _search_doc_by_id(self, index, doc_type, doc_id):
        """Search document in Elasticsearch by _id"""
        result = self.elastic.search(index=index, doc_type=doc_type,
                                     body={
                                         'query': {
                                             'ids': {
                                                 'type': doc_type,
                                                 'values': [u(doc_id)]
                                             }
                                         }
                                     })
        if result['hits']['total'] == 1:
            return result['hits']['hits'][0]
        else:
            return None

    def stop(self):
        """Stop the auto-commit thread."""
        self.auto_commit_interval = None

    def apply_update(self, doc, update_spec):
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return super(DocManager, self).apply_update(doc, update_spec)

    @wrap_exceptions
    def handle_command(self, doc, namespace, timestamp):
        db = namespace.split('.', 1)[0]
        if doc.get('dropDatabase'):
            dbs = self.command_helper.map_db(db)
            for _db in dbs:
                self.elastic.indices.delete(index=_db.lower())

        if doc.get('renameCollection'):
            raise errors.OperationFailed(
                "elastic_doc_manager does not support renaming a mapping.")

        if doc.get('create'):
            db, coll = self.command_helper.map_collection(db, doc['create'])
            if db and coll:
                self.elastic.indices.put_mapping(
                    index=db.lower(), doc_type=coll,
                    body={
                        "_source": {"enabled": True}
                    })

        if doc.get('drop'):
            db, coll = self.command_helper.map_collection(db, doc['drop'])
            if db and coll:
                # This will delete the items in coll, but not get rid of the
                # mapping.
                warnings.warn("Deleting all documents of type %s on index %s."
                              "The mapping definition will persist and must be"
                              "removed manually." % (coll, db))
                responses = streaming_bulk(
                    self.elastic,
                    (dict(result, _op_type='delete') for result in scan(
                        self.elastic, index=db.lower(), doc_type=coll)))
                for ok, resp in responses:
                    if not ok:
                        LOG.error(
                            "Error occurred while deleting ElasticSearch docum"
                            "ent during handling of 'drop' command: %r" % resp)

    @wrap_exceptions
    def update(self, document_id, update_spec, namespace, timestamp):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.
        """
        #import pdb;pdb.set_trace()
        # self.commit()
        # index, doc_type = self._index_and_mapping(namespace)
        # document_id = update_spec['sku']+"_"+str(update_spec['facility_id'])
        # document = self.elastic.get(index="catalog", doc_type="variant", id=u(document_id))
        # updated = updateVariantDoc(self, document_id, update_spec, namespace, timestamp)

        # _id is immutable in MongoDB, so won't have changed in update
        # updated['_id'] = document['_id']
        update_spec['facility_variant_id'] = document_id
        # self.upsert(update_spec, namespace, timestamp)

        # update_spec["_id"] = document_id
        variantDoc = m_facility_variant.find_one({"_id" : document_id})
        if "$set" in update_spec:
            updatedValues = update_spec['$set']
            for item in updatedValues:
                variantDoc[str(item)] = updatedValues[item]

            variantDoc['variant_id'] = str(document_id)
        self.upsert(variantDoc, namespace, timestamp)

        # upsert() strips metadata, so only _id + fields in _source still here
        return update_spec

    @wrap_exceptions
    def upsert(self, doc, namespace, timestamp):
        """Insert a document into Elasticsearch."""
        doc = updateVariantDoc(self, doc)
        if not doc:
            pass

        def docs_to_upsert():
            # Remove metadata and redundant _id
            # index, doc_type = self._index_and_mapping(namespace)
            index = "catalog"
            doc_type = "variant"
            doc_id = u(doc.pop("_id"))
            # Remove parent field
            # parent_id = self._get_parent_id_from_mongodb(index, doc_type,
            #                                              doc)
            document_action = {
                "_index": index,
                "_type": doc_type,
                "_id": doc_id,
                "_source": self._formatter.format_document(doc)
            }
            document_meta = {
                "_index": self.meta_index_name,
                "_type": self.meta_type,
                "_id": doc_id,
                "_source": {
                    "ns": namespace,
                    "_ts": timestamp
                }
            }

            # if parent_id is not None:
            #     document_action["_parent"] = parent_id

            yield document_action
            yield document_meta
        try:
            kw = {}
            if self.chunk_size > 0:
                kw['chunk_size'] = self.chunk_size
            # import pdb;pdb.set_trace()
            responses = streaming_bulk(client=self.elastic,
                                       actions=docs_to_upsert(),
                                       **kw)

            for ok, resp in responses:
                if not ok:
                    LOG.error(
                        "Could not bulk-upsert document "
                        "into ElasticSearch: %r" % resp)
            if self.auto_commit_interval == 0:
                self.commit()
        except errors.EmptyDocsError:
            # This can happen when mongo-connector starts up, there is no
            # config file, but nothing to dump
            pass

    @wrap_exceptions
    def bulk_upsert(self, docs, namespace, timestamp):
        LOG.info("Skip bulk_upsert for facility_variant data!!")
        

    @wrap_exceptions
    def insert_file(self, f, namespace, timestamp):
        doc = f.get_metadata()
        doc_id = str(doc.pop('_id'))
        index, doc_type = self._index_and_mapping(namespace)

        # make sure that elasticsearch treats it like a file
        if not self.has_attachment_mapping:
            body = {
                "properties": {
                    self.attachment_field: {"type": "attachment"}
                }
            }
            self.elastic.indices.put_mapping(index=index,
                                             doc_type=doc_type,
                                             body=body)
            self.has_attachment_mapping = True

        metadata = {
            'ns': namespace,
            '_ts': timestamp,
        }

        # Remove parent id field
        parent_id = self._get_parent_id_from_mongodb(index, doc_type, doc)

        doc = self._formatter.format_document(doc)
        doc[self.attachment_field] = base64.b64encode(f.read()).decode()

        parent_args = {}
        if parent_id is not None:
            parent_args['parent'] = parent_id

        self.elastic.index(
            index=index, doc_type=doc_type, body=doc, id=doc_id,
            refresh=(self.auto_commit_interval == 0), **parent_args)

        self.elastic.index(index=self.meta_index_name, doc_type=self.meta_type,
                           body=bson.json_util.dumps(metadata), id=doc_id,
                           refresh=(self.auto_commit_interval == 0))

    @wrap_exceptions
    def remove(self, document_id, namespace, timestamp):
        """Remove a document from Elasticsearch."""
        index, doc_type = self._index_and_mapping(namespace)

        parent_args = {}
        if self._is_child_type(index, doc_type):
            # We can't use delete() directly here and have to do a full search
            # first. This is due to the fact that Elasticsearch needs the
            # parent ID to know where to route the delete request. We do
            # not have the parent ID available in our remove request though.
            document = self._search_doc_by_id(index, doc_type, document_id)
            if document is None:
                LOG.error('Could not find document with ID "%s" in '
                          'Elasticsearch to apply remove', u(document_id))
                return

            parent_id = self._get_parent_id_from_elastic(document)
            if parent_id is not None:
                parent_args['parent'] = parent_id

        self.elastic.delete(index=index, doc_type=doc_type, id=u(document_id),
                            refresh=(self.auto_commit_interval == 0),
                            **parent_args)

        self.elastic.delete(index=self.meta_index_name, doc_type=self.meta_type,
                            id=u(document_id),
                            refresh=(self.auto_commit_interval == 0))

    @wrap_exceptions
    def _stream_search(self, *args, **kwargs):
        """Helper method for iterating over ES search results."""
        for hit in scan(self.elastic, query=kwargs.pop('body', None),
                        scroll='10m', **kwargs):
            hit['_source']['_id'] = hit['_id']
            if '_parent' in hit:
                hit['_source']['_parent'] = hit['_parent']
            yield hit['_source']

    def search(self, start_ts, end_ts):
        """Query Elasticsearch for documents in a time range.

        This method is used to find documents that may be in conflict during
        a rollback event in MongoDB.
        """
        return self._stream_search(
            index=self.meta_index_name,
            body={
                "query": {
                    "range": {
                        "_ts": {"gte": start_ts, "lte": end_ts}
                    }
                }
            })

    def commit(self):
        """Refresh all Elasticsearch indexes."""
        retry_until_ok(self.elastic.indices.refresh, index="")

    def run_auto_commit(self):
        """Periodically commit to the Elastic server."""
        self.elastic.indices.refresh()
        if self.auto_commit_interval not in [None, 0]:
            Timer(self.auto_commit_interval, self.run_auto_commit).start()

    @wrap_exceptions
    def get_last_doc(self):
        """Get the most recently modified document from Elasticsearch.

        This method is used to help define a time window within which documents
        may be in conflict after a MongoDB rollback.
        """
        try:
            result = self.elastic.search(
                index=self.meta_index_name,
                body={
                    "query": {"match_all": {}},
                    "sort": [{"_ts": "desc"}],
                },
                size=1
            )["hits"]["hits"]
            for r in result:
                r['_source']['_id'] = r['_id']
                return r['_source']
        except es_exceptions.RequestError:
            # no documents so ES returns 400 because of undefined _ts mapping
            return None






# def elastic_doc(doc):
#     docs = []
#     productId = doc['product_id']
#     if productId:
#         product = m_product.find_one({"_id" : productId})
#         facilities = [100,101]
#         for facility in facilities:
#             facilityVariant = m_facility_variant.find_one({"sku" : str(doc['sku']), "facility_id" : facility})
#             if product and facilityVariant:
#                 sku = str(doc['sku'])
#                 name = str(doc['name'])
#                 egSku = str(doc['eg_sku'])
#                 egProdName = str(doc['eg_product_name'])
#                 packType = str(doc['pack_type'])
#                 packSize = doc['pack_size']
#                 isDiscontinue = doc['is_discontinue']
#                 isAvailable = doc['is_available']
#                 isColdStorage = doc['is_cold_storage']
#                 isRetired = doc['is_retired']
#                 status = facilityVariant['status']
#                 drugStrength = doc['drug_strength']
#                 division = product['brand_distribution']
#                 reason = doc['reason']
#                 replacedVariant = doc['replaced_variant']
#                 comment = doc['comment']
#                 classification = product['classification']
#                 rackDetails = facilityVariant['rack_details']
#                 salts = product['salts']
#                 drugType = product['drug_type']
#                 facilityId = facilityVariant['facility_id']
#                 mrp = facilityVariant['mrp']
#                 sellingPrice = facilityVariant['selling_price']
#                 discount = facilityVariant['discount']
#                 thumbnailImage = doc['thumbnail_image']
#                 images = doc['images']
#                 consumptionPerDay = doc['consumption_per_day']

#                 try:
#                     elasticDoc = {
#                         "_id" : sku+"_"+str(facilityId),
#                         "sku" : sku,
#                         "name" : name,
#                         "eg_sku" : egSku,
#                         "eg_product_name" : egProdName,
#                         "pack_type" : packType,
#                         "pack_size" : packSize,
#                         "is_discontinue" : isDiscontinue,
#                         "is_available" : isAvailable,
#                         "is_cold_storage" : isColdStorage,
#                         "is_retired" : isRetired,
#                         "status" : status,
#                         "drug_strength" : drugStrength,
#                         "brand_distribution" : division,
#                         "reason" : reason,
#                         "replaced_variant" : replacedVariant,
#                         "comment" : comment,
#                         "classification" : classification,
#                         "rack_details" : rackDetails,
#                         "salts" : salts,
#                         "drug_type" : drugType,
#                         "facility_id" : facilityId,
#                         "mrp" : mrp,
#                         "selling_price" : sellingPrice,
#                         "discount" : discount,
#                         "thumbnail_image" : thumbnailImage,
#                         "images" : images,
#                         "consumption_per_day" : consumptionPerDay
#                     }
#                 except Exception as e:
#                     print e
                
#                 print "successfull update doc : "+str(elasticDoc)
#                 docs.append(elasticDoc)
#     return docs


def updateVariantDoc(self, update_spec):
    document_id = None
    facilityVariantId = None
    if "facility_variant_id" in update_spec:
        facilityVariantId = update_spec['facility_variant_id']
        if facilityVariantId:
            facilityVariant = m_facility_variant.find_one({"_id" : facilityVariantId})
            if facilityVariant:
                # self.commit()
                if "sku" in facilityVariant and "facility_id" in facilityVariant:

                    document_id = facilityVariant['sku']+"_"+str(facilityVariant['facility_id'])
    else:
        if "sku" in update_spec and "facility_id" in update_spec:
            document_id = update_spec['sku']+"_"+str(update_spec['facility_id'])
            facilityVariantId = update_spec['_id']

        # index, doc_type = self._index_and_mapping(namespace)

        # import pdb; pdb.set_trace()
    if document_id:
        time.delay(10)
        document = self.elastic.get(index="catalog", doc_type="variant", id=u(document_id))
        if document and document['_source']:

            finalUpdatedElasticDoc = document['_source']

            for item in update_spec:
                
                if item in finalUpdatedElasticDoc:
                    finalUpdatedElasticDoc.pop(item)

                finalUpdatedElasticDoc[item] = update_spec[item]

            finalUpdatedElasticDoc['_id'] = document_id
            finalUpdatedElasticDoc['facility_variant_id'] = facilityVariantId
            # print "document['_source'] : "+str(document['_source'])
            # print "finalUpdatedElasticDoc : "+str(finalUpdatedElasticDoc)
            #return self.apply_update(document['_source'], finalUpdatedElasticDoc)
            return finalUpdatedElasticDoc
    else:
        LOG.error("facilityVariant not found: "+str(update_spec))
        return None





    









