# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from enum import Enum

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from array_utils import split

_BQ_CHUNK_SIZE = 1000
_BQ_QUERY_TIMEOUT = 10.0 * 60.0
_BQ_MAX_ROW_TO_DELETE = 2000

class BowlingStatus(Enum):
    SCANNED = 1
    REMOVED = 2
    FAILED_TO_REMOVE = 3


class BqServiceWrapper:

    @property
    def client(self):
        return self._client

    def __init__(self, ds_id):
        self._client = bigquery.Client()
        self._ds_id = ds_id
        self._ds_full_name = f"{self.client.project}.{self._ds_id}"
        self._ds = self.create_dataset(self._ds_full_name)

    def create_dataset(self, dataset_id):
        """Creates dataset"""
        dataset = self.get_dataset(dataset_id)
        if dataset is not None:
            return None
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        dataset = self.client.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(self.client.project, dataset.dataset_id))
        return dataset

    def create_table(self, table_id, schema):
        """Creates table"""
        table_full_name = self.get_table_full_name(table_id)
        table = self.get_table(table_full_name)
        if table is not None:
            return  # self.client.delete_table(table_full_name, not_found_ok=True)  # Make an API
            # request.  # print("Deleted table '{}'.".format(table_full_name))
        table = bigquery.Table(table_full_name, schema=schema)
        table = self.client.create_table(table)  # Make an API request.
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    def delete_table(self, table_id):
        """Deletes dataset"""
        table_full_name = self.get_table_full_name(table_id)
        table = self.get_table(table_full_name)
        if table is not None:
            self.client.delete_table(table_full_name, not_found_ok=True)  # Make an API request.
            print("Deleted table '{}'.".format(table_full_name))

    def get_dataset(self, dataset_full_name):
        """Returns dataset by name"""
        dataset = None
        try:
            dataset = self.client.get_dataset(dataset_full_name)  # Make an API request.
            print("Dataset {} already exists".format(dataset_full_name))
        except NotFound:
            print("Dataset {} is not found".format(dataset_full_name))
        return dataset

    def get_table(self, table_full_name):
        """Returns table by name"""
        table = None
        try:
            table = self.client.get_table(table_full_name)  # Make an API request.
            print("Table {} already exists".format(table_full_name))
        except NotFound:
            print("Table {} is not found".format(table_full_name))
        return table

    def get_table_full_name(self, table_id):
        """Returns table full name"""
        return self._ds_full_name + f".{table_id}"

    def upload_rows_to_bq(self, table_id, rows_to_insert):
        """Inserts rows to BQ"""
        table_full_name = self.get_table_full_name(table_id)
        for ads_chunk in split(rows_to_insert, _BQ_CHUNK_SIZE):
            errors = self.client.insert_rows_json(table_full_name, ads_chunk, row_ids=[None] * len(
                rows_to_insert))  # Make an API request.
            if not errors:
                print("New rows have been added.")
            else:
                print("Encountered errors while inserting rows: {}".format(errors))


    def remove_outdated_scanned_rows(self, table_id):
        """Removes rows with outdated status scanned"""
        affected_rows = 0
        table_full_name = self.get_table_full_name(table_id)
        query_text = f""" DELETE TOP {_BQ_MAX_ROW_TO_DELETE} FROM {table_full_name} o
                            WHERE timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 120 
                            MINUTE) AND timestamp not in ( 
                                                SELECT MAX(timestamp) 
                                                FROM  {table_full_name}  i 
                                                WHERE i.ad_id=o.ad_id 
                                                GROUP  BY ad_id) """
        query_job = self.client.query(query_text, timeout=_BQ_QUERY_TIMEOUT)
        query_job.result()  # Wait for query job to finish.
        print(f"DML query modified {query_job.num_dml_affected_rows} rows.")
        return query_job.num_dml_affected_rows
