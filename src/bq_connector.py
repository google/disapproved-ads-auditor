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

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from array_utils import split

_BQ_CHUNK_SIZE = 1000
_BQ_QUERY_TIMEOUT = 10.0 * 60.0


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
        dataset = self.get_dataset(dataset_id)
        if dataset is not None:
            return None
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        dataset = self.client.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(self.client.project, dataset.dataset_id))
        return dataset

    def create_table(self, table_id, schema):
        table_full_name = self.get_table_full_name(table_id)
        table = self.get_table(table_full_name)
        if table is not None:
            return  # self.client.delete_table(table_full_name, not_found_ok=True)  # Make an API
            # request.  # print("Deleted table '{}'.".format(table_full_name))
        table = bigquery.Table(table_full_name, schema=schema)
        table = self.client.create_table(table)  # Make an API request.
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    def delete_table(self, table_id):
        table_full_name = self.get_table_full_name(table_id)
        table = self.get_table(table_full_name)
        if table is not None:
            self.client.delete_table(table_full_name, not_found_ok=True)  # Make an API request.
            print("Deleted table '{}'.".format(table_full_name))

    def get_dataset(self, dataset_full_name):
        dataset = None
        try:
            dataset = self.client.get_dataset(dataset_full_name)  # Make an API request.
            print("Dataset {} already exists".format(dataset_full_name))
        except NotFound:
            print("Dataset {} is not found".format(dataset_full_name))
        return dataset

    def get_table(self, table_full_name):
        table = None
        try:
            table = self.client.get_table(table_full_name)  # Make an API request.
            print("Table {} already exists".format(table_full_name))
        except NotFound:
            print("Table {} is not found".format(table_full_name))
        return table

    def get_table_full_name(self, table_id):
        return self._ds_full_name + f".{table_id}"

    def upload_rows_to_bq(self, table_id, rows_to_insert):
        table_full_name = self.get_table_full_name(table_id)
        for ads_chunk in split(rows_to_insert, _BQ_CHUNK_SIZE):
            errors = self.client.insert_rows_json(table_full_name, ads_chunk,
                row_ids=[None] * len(rows_to_insert))  # Make an API request.
            if not errors:
                print("New rows have been added.")
            else:
                print("Encountered errors while inserting rows: {}".format(errors))

    def update_bq_ads_status_failed(self, table_id, update_ads):
        affected_rows = 0
        table_full_name = self.get_table_full_name(table_id)
        for update_ads_chunk in split(update_ads, _BQ_CHUNK_SIZE):
            ad_ids = [item["ad_id"] for item in update_ads_chunk]
            removal_errors = [item["removal_error"] for item in update_ads_chunk]
            update_removal_error = " ".join(
                f"WHEN ad_id = '{ad_id}' THEN '{removal_error}'" for ad_id, removal_error in
                zip(ad_ids, removal_errors))
            affected_rows += self.update_bq_ads_status(f"""
                    UPDATE {table_full_name}
                    SET status = 'Failed Removing', 
                    removal_error = CASE {update_removal_error} END
                    WHERE ad_id IN {tuple(ad_ids)}
                """)
        return affected_rows

    def update_bq_ads_status_removed(self, table_id, update_ads):
        affected_rows = 0
        table_full_name = self.get_table_full_name(table_id)
        for update_ads_chunk in split(update_ads, _BQ_CHUNK_SIZE):
            ad_ids = [item["ad_id"] for item in update_ads_chunk]
            affected_rows += self.update_bq_ads_status(f"""
                            UPDATE {table_full_name} 
                            SET status = 'Removed' 
                            WHERE ad_id IN {tuple(ad_ids)} 
                            """)
        return affected_rows

    def update_bq_ads_status(self, query_text):
        query_job = self.client.query(query_text, timeout=_BQ_QUERY_TIMEOUT)
        query_job.result()  # Wait for query job to finish.
        print(f"DML query modified {query_job.num_dml_affected_rows} rows.")
        return query_job.num_dml_affected_rows
