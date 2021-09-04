from google.api_core.exceptions import NotFound
from google.cloud import bigquery


class BqServiceWrapper:

    @property
    def client(self):
        return self._client

    def __init__(self, ds_id):
        self._client = bigquery.Client()
        self._ds_id = ds_id
        self._ds_full_name = f"{self.client.project}.{self._ds_id}"
        self.ds = self.create_dataset(self._ds_full_name)

    def create_dataset(self, dataset_id):
        dataset = self.get_dataset(dataset_id)
        if dataset is not None:
            return
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"

        dataset = self.client.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(self.client.project, dataset.dataset_id))
        return dataset

    def create_table(self, table_id, schema):
        table_full_name = self.get_table_full_name(table_id)
        table = self.get_table(table_full_name)
        if table is not None:
            return
            # self.client.delete_table(table_full_name, not_found_ok=True)  # Make an API request.
            # print("Deleted table '{}'.".format(table_full_name))
        table = bigquery.Table(table_full_name, schema=schema)
        table = self.client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

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
        errors = self.client.insert_rows_json(
                table_full_name, rows_to_insert, row_ids=[None] * len(rows_to_insert))  # Make an API request.
        if not errors:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    def update_bq_ads_status(self, status, table_id, update_ads):
        table_full_name = self.get_table_full_name(table_id)
        query_text = f"""
        UPDATE '{table_full_name}'
        SET status = {status}
        SET removal_error = {[item["removal_error"] for item in update_ads]}
        WHERE account_id IN {[item["ad_id"] for item in update_ads]}
        """
        query_job = self.client.query(query_text)

        # Wait for query job to finish.
        query_job.result()

        print(f"DML query modified {query_job.num_dml_affected_rows} rows.")
        return query_job.num_dml_affected_rows
